"""SQLite storage layer: schema (DDL), transactions, small read helpers."""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .schedule import DatasetDef

DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id               TEXT PRIMARY KEY,
  started_at           TEXT NOT NULL,
  finished_at          TEXT,
  status               TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCESS','PARTIAL_SUCCESS','FAILED','ABANDONED')),
  as_of_ts             TEXT,
  files_seen           INTEGER NOT NULL DEFAULT 0,
  files_loaded         INTEGER NOT NULL DEFAULT 0,
  files_skipped        INTEGER NOT NULL DEFAULT 0,
  files_failed         INTEGER NOT NULL DEFAULT 0,
  rows_read            INTEGER NOT NULL DEFAULT 0,
  rows_loaded          INTEGER NOT NULL DEFAULT 0,
  rows_already_loaded  INTEGER NOT NULL DEFAULT 0,
  rows_rejected        INTEGER NOT NULL DEFAULT 0,
  partitions_evaluated INTEGER NOT NULL DEFAULT 0,
  alerts_open          INTEGER NOT NULL DEFAULT 0,
  duration_seconds     REAL,
  failure_reason       TEXT
);

CREATE TABLE IF NOT EXISTS dataset_registry (
  dataset_id              TEXT PRIMARY KEY,
  dataset_name            TEXT NOT NULL,
  owner                   TEXT NOT NULL,
  frequency               TEXT NOT NULL CHECK (frequency IN ('hourly','daily')),
  expected_offset_minutes INTEGER NOT NULL CHECK (expected_offset_minutes >= 0),
  grace_minutes           INTEGER NOT NULL CHECK (grace_minutes >= 0),
  completion_sla_minutes  INTEGER NOT NULL CHECK (completion_sla_minutes >= grace_minutes),
  min_rows                INTEGER NOT NULL CHECK (min_rows >= 0),
  severity                TEXT NOT NULL CHECK (severity IN ('low','medium','high','critical')),
  definition_hash         TEXT NOT NULL,
  updated_run_id          TEXT NOT NULL
);

-- SLA definitions are never overwritten silently: every change appends a version here.
CREATE TABLE IF NOT EXISTS dataset_registry_history (
  dataset_id              TEXT NOT NULL,
  version                 INTEGER NOT NULL,
  definition_hash         TEXT NOT NULL,
  dataset_name            TEXT NOT NULL,
  owner                   TEXT NOT NULL,
  frequency               TEXT NOT NULL,
  expected_offset_minutes INTEGER NOT NULL,
  grace_minutes           INTEGER NOT NULL,
  completion_sla_minutes  INTEGER NOT NULL,
  min_rows                INTEGER NOT NULL,
  severity                TEXT NOT NULL,
  valid_from_run_id       TEXT NOT NULL,
  valid_from_ts           TEXT NOT NULL,
  PRIMARY KEY (dataset_id, version)
);

CREATE TABLE IF NOT EXISTS file_ingest_log (
  source_file         TEXT NOT NULL,
  file_sha256         TEXT NOT NULL,
  run_id              TEXT NOT NULL,
  status              TEXT NOT NULL CHECK (status IN ('LOADED','FAILED')),
  rows_read           INTEGER NOT NULL DEFAULT 0,
  rows_loaded         INTEGER NOT NULL DEFAULT 0,
  rows_already_loaded INTEGER NOT NULL DEFAULT 0,
  rows_rejected       INTEGER NOT NULL DEFAULT 0,
  warnings            TEXT,
  error               TEXT,
  processed_at        TEXT NOT NULL,
  PRIMARY KEY (source_file, file_sha256)
);

CREATE TABLE IF NOT EXISTS load_events (
  event_id      TEXT PRIMARY KEY,
  dataset_id    TEXT NOT NULL REFERENCES dataset_registry(dataset_id),
  partition_key TEXT NOT NULL,
  event_type    TEXT NOT NULL CHECK (event_type IN ('ARRIVED','COMPLETED','FAILED')),
  event_ts      TEXT NOT NULL,
  row_count     INTEGER CHECK (row_count IS NULL OR row_count >= 0),
  source_file   TEXT NOT NULL,
  source_line   INTEGER NOT NULL,
  run_id        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_ds_part ON load_events(dataset_id, partition_key, event_ts);
CREATE INDEX IF NOT EXISTS idx_events_ds_type_ts ON load_events(dataset_id, event_type, event_ts);

CREATE TABLE IF NOT EXISTS rejects (
  reject_id     TEXT PRIMARY KEY,
  run_id        TEXT NOT NULL,
  entity        TEXT NOT NULL CHECK (entity IN ('event','registry','file')),
  source_file   TEXT NOT NULL,
  source_line   INTEGER NOT NULL,
  reason_code   TEXT NOT NULL,
  reason_detail TEXT,
  raw_record    TEXT
);

-- Snapshot tables: rebuilt by every evaluation (pure function of events + registry + as_of).
CREATE TABLE IF NOT EXISTS partition_sla_status (
  dataset_id               TEXT NOT NULL,
  partition_key            TEXT NOT NULL,
  expected_arrival_ts      TEXT NOT NULL,
  arrival_deadline_ts      TEXT NOT NULL,
  completion_deadline_ts   TEXT NOT NULL,
  arrived_ts               TEXT,
  completed_ts             TEXT,
  arrival_status           TEXT NOT NULL CHECK (arrival_status IN ('ON_TIME','LATE','MISSING','PENDING')),
  completion_status        TEXT NOT NULL CHECK (completion_status IN ('ON_TIME','LATE','FAILED','OVERDUE','PENDING')),
  sla_status               TEXT NOT NULL CHECK (sla_status IN ('MET','BREACHED','PENDING')),
  arrival_delay_minutes    REAL,
  completion_latency_minutes REAL,
  row_count                INTEGER,
  low_row_count            INTEGER NOT NULL DEFAULT 0,
  arrival_inferred         INTEGER NOT NULL DEFAULT 0,
  timestamp_order_anomaly  INTEGER NOT NULL DEFAULT 0,
  evaluated_as_of          TEXT NOT NULL,
  run_id                   TEXT NOT NULL,
  PRIMARY KEY (dataset_id, partition_key),
  FOREIGN KEY (dataset_id) REFERENCES dataset_registry(dataset_id)
);

CREATE TABLE IF NOT EXISTS dataset_freshness (
  dataset_id             TEXT PRIMARY KEY REFERENCES dataset_registry(dataset_id),
  last_completed_ts      TEXT,
  freshness_minutes      REAL,
  max_staleness_minutes  INTEGER NOT NULL,
  freshness_status       TEXT NOT NULL CHECK (freshness_status IN ('FRESH','STALE','NEVER_LOADED')),
  evaluated_as_of        TEXT NOT NULL,
  run_id                 TEXT NOT NULL
);

-- Alerts keep history: one row per (type, dataset, partition), OPEN -> RESOLVED -> (re)OPEN.
CREATE TABLE IF NOT EXISTS alerts (
  alert_id             TEXT PRIMARY KEY,
  alert_type           TEXT NOT NULL,
  severity             TEXT NOT NULL,
  dataset_id           TEXT NOT NULL REFERENCES dataset_registry(dataset_id),
  partition_key        TEXT NOT NULL,           -- '' for dataset-level alerts (STALE_DATASET)
  status               TEXT NOT NULL CHECK (status IN ('OPEN','RESOLVED')),
  first_detected_as_of TEXT NOT NULL,
  last_evaluated_as_of TEXT NOT NULL,
  resolved_as_of       TEXT,
  message              TEXT NOT NULL,
  suggested_action     TEXT NOT NULL,
  detected_run_id      TEXT NOT NULL,
  last_run_id          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status, severity);
"""


class Store:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        # isolation_level=None => autocommit; we issue BEGIN/COMMIT ourselves (see transaction()).
        self.conn = sqlite3.connect(self.db_path, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL" if self.db_path != ":memory:" else "PRAGMA journal_mode = MEMORY")
        self.conn.executescript(DDL)

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def transaction(self):
        """All-or-nothing unit of work. Any exception rolls everything back and is re-raised."""
        self.conn.execute("BEGIN")
        try:
            yield self.conn
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    # -- helpers --------------------------------------------------------------------------
    def load_datasets(self) -> dict[str, DatasetDef]:
        rows = self.conn.execute(
            "SELECT dataset_id, dataset_name, owner, frequency, expected_offset_minutes, grace_minutes,"
            " completion_sla_minutes, min_rows, severity FROM dataset_registry ORDER BY dataset_id"
        ).fetchall()
        return {r["dataset_id"]: DatasetDef(**dict(r)) for r in rows}

    def scalar(self, sql: str, params: tuple = ()):
        row = self.conn.execute(sql, params).fetchone()
        return None if row is None else row[0]
