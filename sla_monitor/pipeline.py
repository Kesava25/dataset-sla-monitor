"""Run orchestration: registry -> ingest files -> evaluate -> metrics. Safe to re-run at any time.

Restart / retry semantics
  * Each file loads in ONE transaction, so a crash leaves no partial file behind.
  * A file (name + sha256) already LOADED is skipped; a FAILED file is retried on the next run.
  * Event ids are content-derived and inserts are INSERT OR IGNORE, so even re-delivering the same
    events in a different file cannot create duplicates.
  * A run left in RUNNING (process killed) is marked ABANDONED by the next run.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable

from .config import Config
from .evaluate import EvalResult, evaluate
from .ingest import (
    FileError, FileResult, REQUIRED_EVENT_COLS, KNOWN_EVENT_COLS, ingest_event_file, is_already_processed,
    load_registry, parse_csv, record_file_failure,
)
from .store import Store
from .timeutil import fmt_ts, utc_now

log = logging.getLogger("sla_monitor")
EXIT_CODES = {"SUCCESS": 0, "FAILED": 1, "PARTIAL_SUCCESS": 2}


@dataclass
class RunResult:
    run_id: str
    status: str
    as_of: str
    started_at: str
    finished_at: str
    duration_seconds: float
    ingest_seconds: float = 0.0
    evaluate_seconds: float = 0.0
    files_seen: int = 0
    files_loaded: int = 0
    files_skipped: int = 0
    files_failed: int = 0
    rows_read: int = 0
    rows_loaded: int = 0
    rows_already_loaded: int = 0
    rows_rejected: int = 0
    registry_rows_rejected: int = 0
    partitions_evaluated: int = 0
    alerts_open: int = 0
    alerts_new: int = 0
    alerts_resolved: int = 0
    failure_reason: str | None = None
    file_results: list[dict] = field(default_factory=list)

    @property
    def exit_code(self) -> int:
        return EXIT_CODES[self.status]


def setup_logging(log_path: str | None, level: int = logging.INFO) -> None:
    log.setLevel(level)
    if log.handlers:
        return
    fmt = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
    fmt.converter = time.gmtime
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    log.addHandler(sh)
    if log_path:
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_path)
        fh.setFormatter(fmt)
        log.addHandler(fh)


def _make_run_id(started: datetime, parts: list[str]) -> str:
    h = hashlib.sha1("|".join(parts).encode()).hexdigest()[:6]
    return f"run_{started:%Y%m%dT%H%M%S}_{h}"


def run_pipeline(cfg: Config, as_of: datetime | None = None, run_id: str | None = None,
                 fault_hook: Callable[[str, str], None] | None = None, do_evaluate: bool = True,
                 now: datetime | None = None) -> RunResult:
    t0 = time.perf_counter()
    started = now or utc_now()
    as_of = as_of or started
    files = cfg.landing_files()
    started_s = fmt_ts(started)
    store = Store(cfg.db_path)
    try:
        if run_id is None:                       # unique even if two runs start in the same second
            base = _make_run_id(started, [fmt_ts(as_of), *[f.name for f in files]])
            run_id, n = base, 1
            while store.scalar("SELECT 1 FROM pipeline_runs WHERE run_id=?", (run_id,)):
                n += 1
                run_id = f"{base}_{n}"
        with store.transaction() as c:   # recovery: anything still RUNNING belongs to a dead process
            c.execute("UPDATE pipeline_runs SET status='ABANDONED', finished_at=?, failure_reason="
                      "'superseded: process ended without finishing' WHERE status='RUNNING'", (started_s,))
            c.execute("INSERT INTO pipeline_runs(run_id,started_at,status,as_of_ts) VALUES (?,?,'RUNNING',?)",
                      (run_id, started_s, fmt_ts(as_of)))
        res = RunResult(run_id, "RUNNING", fmt_ts(as_of), started_s, "", 0.0, files_seen=len(files))
        log.info("run_id=%s start as_of=%s files=%d", run_id, res.as_of, len(files))
        try:
            _execute(cfg, store, res, files, as_of, fault_hook, do_evaluate, started_s)
        except Exception as exc:                                    # run-level failure
            res.status, res.failure_reason = "FAILED", f"{type(exc).__name__}: {exc}"
            log.error("run_id=%s FAILED reason=%s", run_id, res.failure_reason)
        res.finished_at = fmt_ts(utc_now())
        res.duration_seconds = round(time.perf_counter() - t0, 3)
        with store.transaction() as c:
            c.execute(
                "UPDATE pipeline_runs SET finished_at=?,status=?,files_seen=?,files_loaded=?,files_skipped=?,files_failed=?,"
                "rows_read=?,rows_loaded=?,rows_already_loaded=?,rows_rejected=?,partitions_evaluated=?,alerts_open=?,"
                "duration_seconds=?,failure_reason=? WHERE run_id=?",
                (res.finished_at, res.status, res.files_seen, res.files_loaded, res.files_skipped, res.files_failed,
                 res.rows_read, res.rows_loaded, res.rows_already_loaded, res.rows_rejected, res.partitions_evaluated,
                 res.alerts_open, res.duration_seconds, res.failure_reason, run_id))
        log.info("run_id=%s end status=%s read=%d loaded=%d already_loaded=%d rejected=%d files_failed=%d "
                 "partitions=%d alerts_open=%d duration=%.2fs", run_id, res.status, res.rows_read, res.rows_loaded,
                 res.rows_already_loaded, res.rows_rejected, res.files_failed, res.partitions_evaluated,
                 res.alerts_open, res.duration_seconds)
        out = Path(cfg.output_dir)
        out.mkdir(parents=True, exist_ok=True)
        (out / "run_summary.json").write_text(json.dumps(asdict(res), indent=2) + "\n")
        return res
    finally:
        store.close()


def _execute(cfg, store, res, files, as_of, fault_hook, do_evaluate, started_s):
    t_ing = time.perf_counter()
    # 1. registry ------------------------------------------------------------------------------
    reg_path = Path(cfg.registry_path)
    registry_failed = False
    if reg_path.exists():
        try:
            rr = load_registry(store, reg_path, res.run_id, started_s)
            res.registry_rows_rejected = rr.rejected
            log.info("registry rows=%d accepted=%d new_or_changed=%d rejected=%d", rr.rows_read, rr.accepted,
                     rr.new_or_changed, rr.rejected)
        except FileError as err:
            registry_failed = True
            log.error("registry file unusable: %s", err)
            record_file_failure(store, res.run_id, started_s, reg_path.name, _sha_or_blank(reg_path), err, entity="file")
    datasets = store.load_datasets()
    if not datasets:
        raise RuntimeError("no dataset registry available (file missing/unusable and none stored)")
    if registry_failed:
        res.files_failed += 1

    # 2. event files -----------------------------------------------------------------------------
    for path in files:
        try:
            parsed = parse_csv(path, REQUIRED_EVENT_COLS, KNOWN_EVENT_COLS)
        except FileError as err:
            sha = _sha_or_blank(path)
            record_file_failure(store, res.run_id, started_s, path.name, sha, err)
            res.files_failed += 1
            res.file_results.append(asdict(FileResult(path.name, sha, "FAILED", error=str(err))))
            log.error("file=%s FAILED %s", path.name, err)
            continue
        if is_already_processed(store, path.name, parsed.sha256):
            res.files_skipped += 1
            res.file_results.append(asdict(FileResult(path.name, parsed.sha256, "SKIPPED")))
            log.info("file=%s skipped (already loaded, same sha256)", path.name)
            continue
        try:
            fr = ingest_event_file(store, path, datasets, res.run_id, started_s, parsed, fault_hook)
        except Exception as exc:                                    # transaction already rolled back
            record_file_failure(store, res.run_id, started_s, path.name, parsed.sha256, exc)
            res.files_failed += 1
            res.file_results.append(asdict(FileResult(path.name, parsed.sha256, "FAILED", error=repr(exc))))
            log.error("file=%s FAILED (rolled back) %r", path.name, exc)
            continue
        res.files_loaded += 1
        res.rows_read += fr.rows_read
        res.rows_loaded += fr.rows_loaded
        res.rows_already_loaded += fr.rows_already_loaded
        res.rows_rejected += fr.rows_rejected
        res.file_results.append(asdict(fr))
        for w in fr.warnings:
            log.warning("file=%s %s", path.name, w)
        log.info("file=%s read=%d loaded=%d already_loaded=%d rejected=%d", fr.source_file, fr.rows_read,
                 fr.rows_loaded, fr.rows_already_loaded, fr.rows_rejected)
    if not files:
        log.warning("no input files found in %s (nothing to ingest)", cfg.landing_dir)
    res.ingest_seconds = round(time.perf_counter() - t_ing, 3)

    # 3. evaluate ---------------------------------------------------------------------------------
    if do_evaluate:
        t_ev = time.perf_counter()
        ev: EvalResult = evaluate(store, as_of, cfg.lookback_hours, res.run_id)
        res.partitions_evaluated, res.alerts_open = ev.partitions, ev.alerts_open
        res.alerts_new, res.alerts_resolved = ev.alerts_new, ev.alerts_resolved
        res.evaluate_seconds = round(time.perf_counter() - t_ev, 3)
        log.info("evaluated partitions=%d alerts_open=%d new=%d resolved=%d stale_datasets=%d", ev.partitions,
                 ev.alerts_open, ev.alerts_new, ev.alerts_resolved, ev.stale_datasets)

    attempted = res.files_loaded + res.files_failed
    if res.files_failed == 0:
        res.status = "SUCCESS"
    elif res.files_loaded == 0 and res.files_skipped == 0:
        res.status = "FAILED"
        res.failure_reason = "every input file failed"
    else:
        res.status = "PARTIAL_SUCCESS"
        res.failure_reason = f"{res.files_failed} of {attempted + res.files_skipped} file(s) failed; see file_ingest_log/rejects"


def _sha_or_blank(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return "unreadable"
