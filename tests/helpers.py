import csv
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from sla_monitor.config import Config
from sla_monitor.pipeline import run_pipeline
from sla_monitor.store import Store
from sla_monitor.timeutil import parse_ts

EVENT_HEADER = ["dataset_id", "partition_key", "event_type", "event_ts", "row_count"]
REG_HEADER = ["dataset_id", "dataset_name", "owner", "frequency", "expected_offset_minutes", "grace_minutes",
              "completion_sla_minutes", "min_rows", "severity"]
# daily  partition 2026-01-15: expected 02:00, arrival deadline 02:30, completion deadline 03:30
# hourly partition 2026-01-15T05: expected 05:10, arrival deadline 05:25, completion deadline 05:50
DEFAULT_REGISTRY = [
    ["ds_daily", "Daily feed", "team_a", "daily", "120", "30", "90", "100", "high"],
    ["ds_hourly", "Hourly feed", "team_b", "hourly", "10", "15", "40", "10", "medium"],
]
T = parse_ts("2026-01-15T12:00:00Z")


def write_csv(path, header, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        if header is not None:
            w.writerow(header)
        w.writerows(rows)


class PipelineCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="slatest_"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.cfg = Config(db_path=str(self.tmp / "t.db"), registry_path=str(self.tmp / "dataset_registry.csv"),
                          landing_dir=str(self.tmp / "landing"), output_dir=str(self.tmp / "out"),
                          lookback_hours=48, log_path=None)
        (self.tmp / "landing").mkdir()
        self.write_registry(DEFAULT_REGISTRY)

    def write_registry(self, rows):
        write_csv(self.cfg.registry_path, REG_HEADER, rows)

    def events(self, name, rows, header=EVENT_HEADER):
        write_csv(self.tmp / "landing" / name, header, rows)

    def run_pipe(self, as_of=T, **kw):
        return run_pipeline(self.cfg, as_of=as_of, **kw)

    def db(self):
        return Store(self.cfg.db_path)

    def q(self, sql, params=()):
        s = self.db()
        try:
            return [tuple(r) for r in s.conn.execute(sql, params).fetchall()]
        finally:
            s.close()

    def one(self, sql, params=()):
        return self.q(sql, params)[0][0]

    def status(self, ds, key):
        r = self.q("SELECT arrival_status,completion_status,sla_status FROM partition_sla_status "
                   "WHERE dataset_id=? AND partition_key=?", (ds, key))
        return r[0] if r else None

    def snapshot(self):
        """Everything a rerun must not change (excludes run bookkeeping columns)."""
        return {
            "events": self.q("SELECT event_id,dataset_id,partition_key,event_type,event_ts,row_count,source_file,source_line "
                             "FROM load_events ORDER BY event_id"),
            "rejects": self.q("SELECT reject_id,entity,source_file,source_line,reason_code FROM rejects ORDER BY reject_id"),
            "status": self.q("SELECT * FROM partition_sla_status ORDER BY dataset_id,partition_key").__len__(),
            "status_rows": self.q("SELECT dataset_id,partition_key,arrival_status,completion_status,sla_status,"
                                  "arrived_ts,completed_ts,row_count FROM partition_sla_status ORDER BY 1,2"),
            "alerts": self.q("SELECT alert_id,status,first_detected_as_of,message FROM alerts ORDER BY alert_id"),
            "freshness": self.q("SELECT dataset_id,last_completed_ts,freshness_status FROM dataset_freshness ORDER BY 1"),
        }


def ev(ds, key, et, ts, rc=""):
    return [ds, key, et, ts, str(rc)]
