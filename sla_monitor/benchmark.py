"""Performance measurement: end-to-end timings on a large synthetic run + one measured optimisation."""
from __future__ import annotations

import resource
import tempfile
import time
from pathlib import Path

from .config import Config
from .generator import GenParams, generate
from .pipeline import run_pipeline
from .store import Store
from .timeutil import parse_ts

INSERT = ("INSERT OR IGNORE INTO load_events(event_id,dataset_id,partition_key,event_type,event_ts,row_count,"
          "source_file,source_line,run_id) VALUES (?,?,?,?,?,?,?,?,?)")


def insert_strategy_comparison(n_naive: int = 3000, n_batched: int = 100000) -> dict:
    """Optimisation measured: row-at-a-time autocommit INSERTs vs one transaction + executemany."""
    def rows(n):
        return [(f"evt_{i:012d}", "ds_bench", "2026-01-01", "ARRIVED", "2026-01-01T00:00:00Z", None, "f.csv", i, "r")
                for i in range(n)]

    def fresh(tmp, name):
        s = Store(Path(tmp) / name)
        s.conn.execute("INSERT INTO dataset_registry VALUES ('ds_bench','b','o','daily',0,0,0,0,'low','h','r')")
        return s

    with tempfile.TemporaryDirectory() as tmp:
        s = fresh(tmp, "naive.db")
        data = rows(n_naive)
        t = time.perf_counter()
        for r in data:                                   # autocommit: one fsync'd commit per row
            s.conn.execute(INSERT, r)
        naive = n_naive / (time.perf_counter() - t)
        s.close()
        s = fresh(tmp, "batched.db")
        data = rows(n_batched)
        t = time.perf_counter()
        with s.transaction() as c:
            for i in range(0, len(data), 5000):
                c.executemany(INSERT, data[i:i + 5000])
        batched = n_batched / (time.perf_counter() - t)
        s.close()
    return {"naive_rows_per_sec": round(naive), "batched_rows_per_sec": round(batched),
            "speedup_x": round(batched / naive, 1)}


def run_benchmark(datasets: int = 60, days: int = 90, seed: int = 42) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        t = time.perf_counter()
        gen = generate(Path(tmp) / "data", GenParams(seed=seed, datasets=datasets, days=days))
        gen_s = time.perf_counter() - t
        cfg = Config(db_path=str(Path(tmp) / "bench.db"), registry_path=str(Path(tmp) / "data/dataset_registry.csv"),
                     landing_dir=str(Path(tmp) / "data/landing"), output_dir=str(Path(tmp) / "out"),
                     lookback_hours=days * 24, log_path=None)
        res = run_pipeline(cfg, as_of=parse_ts(GenParams().as_of))
        t = time.perf_counter()
        res2 = run_pipeline(cfg, as_of=parse_ts(GenParams().as_of))      # idempotent rerun: skip everything
        rerun_s = time.perf_counter() - t
        size_mb = (Path(tmp) / "bench.db").stat().st_size / 1e6
    out = {
        "datasets": datasets, "days": days, "input_rows": gen["totals"]["rows_read"], "partitions": gen["partitions"],
        "generate_seconds": round(gen_s, 2), "ingest_seconds": res.ingest_seconds, "evaluate_seconds": res.evaluate_seconds,
        "pipeline_total_seconds": res.duration_seconds,
        "ingest_rows_per_sec": round(gen["totals"]["rows_read"] / max(res.ingest_seconds, 1e-9)),
        "evaluate_partitions_per_sec": round(res.partitions_evaluated / max(res.evaluate_seconds, 1e-9)),
        "rerun_seconds_all_files_skipped": round(rerun_s, 2), "rerun_status": res2.status,
        "rerun_rows_loaded": res2.rows_loaded, "db_size_mb": round(size_mb, 1),
        "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024),
        "status": res.status, "alerts_open": res.alerts_open,
    }
    out["optimisation_insert_strategy"] = insert_strategy_comparison()
    return out
