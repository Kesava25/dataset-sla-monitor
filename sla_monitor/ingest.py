"""Ingestion: parse CSV files, validate rows, quarantine rejects, load atomically per file."""
from __future__ import annotations

import csv
import hashlib
import io
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .schedule import DatasetDef
from .store import Store
from .validation import (
    KNOWN_EVENT_COLS, REGISTRY_COLS, REQUIRED_EVENT_COLS, RecordError,
    validate_event_row, validate_registry_row,
)

CHUNK = 5000


class FileError(Exception):
    """File-level failure: the whole file is unusable (nothing from it is loaded)."""

    def __init__(self, code: str, detail: str):
        super().__init__(f"{code}: {detail}")
        self.code, self.detail = code, detail


@dataclass
class ParsedFile:
    sha256: str
    header: list[str]
    rows: list[tuple[int, list[str]]]          # (source_line, fields); blank lines already dropped
    warnings: list[str] = field(default_factory=list)


@dataclass
class FileResult:
    source_file: str
    sha256: str
    status: str                                 # LOADED | FAILED | SKIPPED
    rows_read: int = 0
    rows_loaded: int = 0
    rows_already_loaded: int = 0
    rows_rejected: int = 0
    warnings: list[str] = field(default_factory=list)
    error: str | None = None


def parse_csv(path: Path, required: tuple[str, ...], known: tuple[str, ...]) -> ParsedFile:
    """Read + structurally validate a CSV file. Raises FileError for file-level problems."""
    data = path.read_bytes()
    sha = hashlib.sha256(data).hexdigest()
    if not data.strip():
        raise FileError("EMPTY_FILE", "file has no content (no header row)")
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise FileError("NOT_UTF8_TEXT", str(exc)) from exc
    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        header = [h.strip().lower() for h in next(reader)]
        if len(set(header)) != len(header):
            raise FileError("SCHEMA_MISMATCH", f"duplicate column names in header: {header}")
        missing = [c for c in required if c not in header]
        if missing:
            raise FileError("SCHEMA_MISMATCH", f"missing required columns {missing}; header was {header}")
        warnings = []
        extra = [c for c in header if c not in known]
        if extra:
            warnings.append(f"unexpected extra columns ignored: {extra}")
        rows = []
        for fields in reader:
            if not fields:                       # blank line
                continue
            rows.append((reader.line_num, fields))
    except csv.Error as exc:
        raise FileError("MALFORMED_FILE", str(exc)) from exc
    return ParsedFile(sha, header, rows, warnings)


def _reject_id(source_file: str, sha: str, line: int, code: str) -> str:
    return "rej_" + hashlib.sha1(f"{source_file}|{sha}|{line}|{code}".encode()).hexdigest()[:16]


def _row_dict(header: list[str], fields: list[str]) -> dict[str, str]:
    if len(fields) != len(header):
        raise RecordError("MALFORMED_ROW", f"expected {len(header)} fields, got {len(fields)}")
    return dict(zip(header, fields))


def record_file_failure(store: Store, run_id: str, now_iso: str, source_file: str, sha: str,
                        err: Exception, entity: str = "file") -> None:
    """Persist a file-level failure so it is traceable and eligible for retry (own transaction)."""
    code = err.code if isinstance(err, FileError) else "UNEXPECTED_ERROR"
    detail = err.detail if isinstance(err, FileError) else f"{type(err).__name__}: {err}"
    with store.transaction() as c:
        c.execute(
            "INSERT OR REPLACE INTO file_ingest_log(source_file,file_sha256,run_id,status,error,processed_at)"
            " VALUES (?,?,?, 'FAILED', ?, ?)", (source_file, sha, run_id, f"{code}: {detail}", now_iso))
        c.execute(
            "INSERT OR IGNORE INTO rejects(reject_id,run_id,entity,source_file,source_line,reason_code,reason_detail)"
            " VALUES (?,?,?,?,0,?,?)", (_reject_id(source_file, sha, 0, code), run_id, entity, source_file, code, detail))


def is_already_processed(store: Store, source_file: str, sha: str) -> bool:
    return store.scalar(
        "SELECT 1 FROM file_ingest_log WHERE source_file=? AND file_sha256=? AND status='LOADED'",
        (source_file, sha)) is not None


def ingest_event_file(store: Store, path: Path, datasets: dict[str, DatasetDef], run_id: str,
                      now_iso: str, parsed: ParsedFile,
                      fault_hook: Callable[[str, str], None] | None = None) -> FileResult:
    """Validate and load one parsed event file in ONE transaction (all-or-nothing)."""
    name = path.name
    seen: dict[str, tuple[int, int | None]] = {}   # event_id -> (line, row_count) of first occurrence
    valid: list[tuple] = []
    rejects: list[tuple] = []
    for line, fields in parsed.rows:
        try:
            ev = validate_event_row(_row_dict(parsed.header, fields), datasets)
            first = seen.get(ev["event_id"])
            if first is not None:
                note = "same row_count" if first[1] == ev["row_count"] else (
                    f"CONFLICTING row_count ({first[1]} vs {ev['row_count']}); first occurrence kept")
                raise RecordError("DUPLICATE_EVENT", f"duplicate of line {first[0]} ({note})")
            seen[ev["event_id"]] = (line, ev["row_count"])
            valid.append((ev["event_id"], ev["dataset_id"], ev["partition_key"], ev["event_type"],
                          ev["event_ts"], ev["row_count"], name, line, run_id))
        except RecordError as err:
            rejects.append((_reject_id(name, parsed.sha256, line, err.code), run_id, "event", name, line,
                            err.code, err.detail, json.dumps(fields)))

    with store.transaction() as c:
        inserted = 0
        for i in range(0, len(valid), CHUNK):
            before = c.total_changes
            c.executemany(
                "INSERT OR IGNORE INTO load_events(event_id,dataset_id,partition_key,event_type,event_ts,"
                "row_count,source_file,source_line,run_id) VALUES (?,?,?,?,?,?,?,?,?)", valid[i:i + CHUNK])
            inserted += c.total_changes - before
        c.executemany(
            "INSERT OR IGNORE INTO rejects(reject_id,run_id,entity,source_file,source_line,reason_code,"
            "reason_detail,raw_record) VALUES (?,?,?,?,?,?,?,?)", rejects)
        res = FileResult(name, parsed.sha256, "LOADED", rows_read=len(parsed.rows), rows_loaded=inserted,
                         rows_already_loaded=len(valid) - inserted, rows_rejected=len(rejects),
                         warnings=list(parsed.warnings))
        if res.rows_read == 0:
            res.warnings.append("empty batch: header only, no data rows")
        c.execute(
            "INSERT OR REPLACE INTO file_ingest_log(source_file,file_sha256,run_id,status,rows_read,rows_loaded,"
            "rows_already_loaded,rows_rejected,warnings,error,processed_at) VALUES (?,?,?,'LOADED',?,?,?,?,?,NULL,?)",
            (name, parsed.sha256, run_id, res.rows_read, res.rows_loaded, res.rows_already_loaded,
             res.rows_rejected, "; ".join(res.warnings) or None, now_iso))
        if fault_hook:                                # test seam: simulate a crash before COMMIT
            fault_hook(name, "before_commit")
    return res


# ---------------------------------------------------------------------------------------------
@dataclass
class RegistryResult:
    rows_read: int = 0
    accepted: int = 0
    new_or_changed: int = 0
    rejected: int = 0
    warnings: list[str] = field(default_factory=list)


def load_registry(store: Store, path: Path, run_id: str, now_iso: str) -> RegistryResult:
    """Upsert the dataset registry; every definition change appends a history version."""
    parsed = parse_csv(path, REGISTRY_COLS, REGISTRY_COLS)
    res = RegistryResult(rows_read=len(parsed.rows), warnings=list(parsed.warnings))
    name = path.name
    defs: list[DatasetDef] = []
    rejects: list[tuple] = []
    seen: dict[str, int] = {}
    for line, fields in parsed.rows:
        try:
            d = validate_registry_row(_row_dict(parsed.header, fields))
            if d.dataset_id in seen:
                raise RecordError("DUPLICATE_DATASET_ID", f"duplicate of line {seen[d.dataset_id]}")
            seen[d.dataset_id] = line
            defs.append(d)
        except RecordError as err:
            rejects.append((_reject_id(name, parsed.sha256, line, err.code), run_id, "registry", name, line,
                            err.code, err.detail, json.dumps(fields)))
    res.accepted, res.rejected = len(defs), len(rejects)
    with store.transaction() as c:
        for d in defs:
            h = hashlib.sha1(json.dumps(d.__dict__, sort_keys=True).encode()).hexdigest()[:16]
            cur = c.execute("SELECT definition_hash FROM dataset_registry WHERE dataset_id=?", (d.dataset_id,)).fetchone()
            if cur is not None and cur[0] == h:
                continue                                  # unchanged => idempotent no-op
            res.new_or_changed += 1
            vals = (d.dataset_name, d.owner, d.frequency, d.expected_offset_minutes, d.grace_minutes,
                    d.completion_sla_minutes, d.min_rows, d.severity)
            c.execute(
                "INSERT INTO dataset_registry(dataset_id,dataset_name,owner,frequency,expected_offset_minutes,"
                "grace_minutes,completion_sla_minutes,min_rows,severity,definition_hash,updated_run_id)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(dataset_id) DO UPDATE SET dataset_name=excluded.dataset_name,"
                "owner=excluded.owner,frequency=excluded.frequency,expected_offset_minutes=excluded.expected_offset_minutes,"
                "grace_minutes=excluded.grace_minutes,completion_sla_minutes=excluded.completion_sla_minutes,"
                "min_rows=excluded.min_rows,severity=excluded.severity,definition_hash=excluded.definition_hash,"
                "updated_run_id=excluded.updated_run_id", (d.dataset_id, *vals, h, run_id))
            version = (c.execute("SELECT COALESCE(MAX(version),0) FROM dataset_registry_history WHERE dataset_id=?",
                                 (d.dataset_id,)).fetchone()[0]) + 1
            c.execute(
                "INSERT INTO dataset_registry_history(dataset_id,version,definition_hash,dataset_name,owner,frequency,"
                "expected_offset_minutes,grace_minutes,completion_sla_minutes,min_rows,severity,valid_from_run_id,"
                "valid_from_ts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (d.dataset_id, version, h, *vals, run_id, now_iso))
        c.executemany(
            "INSERT OR IGNORE INTO rejects(reject_id,run_id,entity,source_file,source_line,reason_code,reason_detail,"
            "raw_record) VALUES (?,?,?,?,?,?,?,?)", rejects)
    return res
