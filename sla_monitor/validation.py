"""Record-level validation. Every rule raises RecordError(code, detail) -> goes to quarantine.

Rule order for events is fixed (documented in docs/DATA_DICTIONARY.md) so that each bad record
maps to exactly one, deterministic reason code.
"""
from __future__ import annotations

import hashlib
import re

from .schedule import FREQUENCIES, SEVERITIES, DatasetDef
from .timeutil import fmt_ts, parse_ts

EVENT_TYPES = ("ARRIVED", "COMPLETED", "FAILED")
REQUIRED_EVENT_COLS = ("dataset_id", "partition_key", "event_type", "event_ts")
KNOWN_EVENT_COLS = REQUIRED_EVENT_COLS + ("row_count",)
REGISTRY_COLS = (
    "dataset_id", "dataset_name", "owner", "frequency", "expected_offset_minutes",
    "grace_minutes", "completion_sla_minutes", "min_rows", "severity",
)
_SNAKE = re.compile(r"^[a-z][a-z0-9_]*$")
_UINT = re.compile(r"^\d+$")


class RecordError(Exception):
    def __init__(self, code: str, detail: str):
        super().__init__(f"{code}: {detail}")
        self.code, self.detail = code, detail


def event_id_for(dataset_id: str, partition_key: str, event_type: str, event_ts_iso: str) -> str:
    """Deterministic natural-key hash. Same event re-delivered in any file => same id."""
    h = hashlib.sha1("|".join((dataset_id, partition_key, event_type, event_ts_iso)).encode()).hexdigest()
    return "evt_" + h[:16]


def validate_event_row(raw: dict[str, str], datasets: dict[str, DatasetDef]) -> dict:
    g = {k: (raw.get(k) or "").strip() for k in KNOWN_EVENT_COLS}
    event_type = g["event_type"].upper()
    # 1. required fields (row_count is required only for COMPLETED)
    for col in REQUIRED_EVENT_COLS:
        if not g[col]:
            raise RecordError("MISSING_REQUIRED_FIELD", col)
    if event_type == "COMPLETED" and not g["row_count"]:
        raise RecordError("MISSING_REQUIRED_FIELD", "row_count (required for COMPLETED)")
    # 2. enum
    if event_type not in EVENT_TYPES:
        raise RecordError("INVALID_EVENT_TYPE", g["event_type"])
    # 3. referential integrity
    ds = datasets.get(g["dataset_id"])
    if ds is None:
        raise RecordError("UNKNOWN_DATASET", g["dataset_id"])
    # 4. timestamp
    try:
        ts = fmt_ts(parse_ts(g["event_ts"]))
    except ValueError as exc:
        raise RecordError("INVALID_TIMESTAMP", str(exc)) from exc
    # 5. partition key vs dataset frequency
    try:
        ds.partition_start(g["partition_key"])
    except ValueError as exc:
        raise RecordError("INVALID_PARTITION_KEY", str(exc)) from exc
    # 6. row_count
    row_count = None
    if g["row_count"]:
        if not _UINT.match(g["row_count"]):
            raise RecordError("INVALID_ROW_COUNT", f"must be a non-negative integer, got {g['row_count']!r}")
        row_count = int(g["row_count"])
    return {
        "event_id": event_id_for(ds.dataset_id, g["partition_key"], event_type, ts),
        "dataset_id": ds.dataset_id,
        "partition_key": g["partition_key"],
        "event_type": event_type,
        "event_ts": ts,
        "row_count": row_count,
    }


def validate_registry_row(raw: dict[str, str]) -> DatasetDef:
    g = {k: (raw.get(k) or "").strip() for k in REGISTRY_COLS}
    for col in REGISTRY_COLS:
        if not g[col]:
            raise RecordError("MISSING_REQUIRED_FIELD", col)
    if not _SNAKE.match(g["dataset_id"]):
        raise RecordError("INVALID_DATASET_ID", f"must be snake_case, got {g['dataset_id']!r}")
    freq = g["frequency"].lower()
    if freq not in FREQUENCIES:
        raise RecordError("INVALID_FREQUENCY", f"must be one of {FREQUENCIES}, got {g['frequency']!r}")
    sev = g["severity"].lower()
    if sev not in SEVERITIES:
        raise RecordError("INVALID_SEVERITY", f"must be one of {SEVERITIES}, got {g['severity']!r}")
    nums = {}
    for col in ("expected_offset_minutes", "grace_minutes", "completion_sla_minutes", "min_rows"):
        if not _UINT.match(g[col]):
            raise RecordError("INVALID_NUMBER", f"{col} must be a non-negative integer, got {g[col]!r}")
        nums[col] = int(g[col])
    limit = 59 if freq == "hourly" else 1439
    if nums["expected_offset_minutes"] > limit:
        raise RecordError("INVALID_SLA_DEFINITION", f"expected_offset_minutes must be 0..{limit} for {freq}")
    if nums["completion_sla_minutes"] < nums["grace_minutes"]:
        raise RecordError("INVALID_SLA_DEFINITION", "completion_sla_minutes must be >= grace_minutes")
    return DatasetDef(
        dataset_id=g["dataset_id"], dataset_name=g["dataset_name"], owner=g["owner"], frequency=freq,
        severity=sev, **nums,
    )
