from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .benchmark import run_benchmark
from .config import Config
from .evaluate import evaluate
from .generator import GenParams, generate
from .pipeline import run_pipeline, setup_logging
from .report import write_reports
from .store import Store
from .timeutil import parse_ts, utc_now


def _cfg(a) -> Config:
    dd = Path(a.data_dir)
    return Config(db_path=a.db, registry_path=a.registry or str(dd / "dataset_registry.csv"),
                  landing_dir=a.landing or str(dd / "landing"), output_dir=a.output_dir,
                  lookback_hours=a.lookback_hours, log_path=str(Path(a.output_dir) / "pipeline.log"))


def _common(p):
    p.add_argument("--data-dir", default="data/sample", help="folder holding dataset_registry.csv, landing/, expected/")
    p.add_argument("--registry", help="override registry csv path")
    p.add_argument("--landing", help="override landing directory")
    p.add_argument("--db", default="output/sla_monitor.db")
    p.add_argument("--output-dir", default="output")
    p.add_argument("--as-of", help="evaluation instant, ISO-8601 with timezone (default: now)")
    p.add_argument("--lookback-hours", type=int, default=168)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="sla_monitor", description="Dataset arrival/completion SLA monitor")
    sub = ap.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="create synthetic input data + answer key")
    g.add_argument("--out", default="data/sample")
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--datasets", type=int, default=8)
    g.add_argument("--days", type=int, default=7)
    g.add_argument("--as-of", default=GenParams.as_of)
    g.add_argument("--hourly-fraction", type=float, default=0.5)
    g.add_argument("--no-anomalies", action="store_true")

    r = sub.add_parser("run", help="ingest + evaluate + write reports")
    _common(r)
    r.add_argument("--no-report", action="store_true")

    e = sub.add_parser("evaluate", help="re-evaluate SLAs from stored events at --as-of (no ingestion)")
    _common(e)

    rp = sub.add_parser("report", help="export CSVs and write validation_report.md from the database")
    _common(rp)

    b = sub.add_parser("benchmark", help="large-volume timing run")
    b.add_argument("--datasets", type=int, default=60)
    b.add_argument("--days", type=int, default=90)
    b.add_argument("--seed", type=int, default=42)
    b.add_argument("--out", help="also write the result as JSON here")

    a = ap.parse_args(argv)

    if a.cmd == "generate":
        s = generate(a.out, GenParams(a.seed, a.datasets, a.days, a.as_of, a.hourly_fraction, not a.no_anomalies))
        print(json.dumps({k: s[k] for k in ("partitions", "totals", "reject_reason_counts", "scenario_counts")}, indent=2))
        return 0

    if a.cmd == "benchmark":
        res = run_benchmark(a.datasets, a.days, a.seed)
        txt = json.dumps(res, indent=2)
        print(txt)
        if a.out:
            Path(a.out).parent.mkdir(parents=True, exist_ok=True)
            Path(a.out).write_text(txt + "\n")
        return 0

    cfg = _cfg(a)
    setup_logging(cfg.log_path)
    as_of = parse_ts(a.as_of) if a.as_of else utc_now()
    expected = Path(a.data_dir) / "expected"

    if a.cmd == "run":
        res = run_pipeline(cfg, as_of=as_of)
        print(f"run {res.run_id}: {res.status} | read={res.rows_read} loaded={res.rows_loaded} "
              f"already_loaded={res.rows_already_loaded} rejected={res.rows_rejected} "
              f"files_failed={res.files_failed} files_skipped={res.files_skipped} "
              f"partitions={res.partitions_evaluated} alerts_open={res.alerts_open}")
        if res.failure_reason:
            print(f"reason: {res.failure_reason}")
        if not a.no_report:
            store = Store(cfg.db_path)
            try:
                path, ok = write_reports(store, cfg.output_dir, expected if expected.exists() else None)
            finally:
                store.close()
            print(f"report: {path}  validation={'PASS' if ok else 'FAIL'}")
        return res.exit_code

    store = Store(cfg.db_path)
    try:
        if a.cmd == "evaluate":
            ev = evaluate(store, as_of, cfg.lookback_hours, f"eval_{utc_now():%Y%m%dT%H%M%S}")
            print(f"evaluated partitions={ev.partitions} alerts_open={ev.alerts_open} new={ev.alerts_new} "
                  f"resolved={ev.alerts_resolved} stale_datasets={ev.stale_datasets}")
        elif a.cmd == "report":
            path, ok = write_reports(store, cfg.output_dir, expected if expected.exists() else None)
            print(f"report: {path}  validation={'PASS' if ok else 'FAIL'}")
            return 0 if ok else 1
    finally:
        store.close()
    return 0
