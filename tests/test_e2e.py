"""End-to-end: generator -> pipeline -> reconciliation + answer-key comparison."""
import dataclasses
import hashlib
import io
import json
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from sla_monitor import evaluate as evaluate_mod
from sla_monitor.cli import main
from sla_monitor.config import Config
from sla_monitor.generator import GenParams, generate
from sla_monitor.pipeline import run_pipeline
from sla_monitor.report import compare_labels, reconcile
from sla_monitor.store import Store
from sla_monitor.timeutil import parse_ts


def tree_hash(root: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(root.rglob("*")):
        if p.is_file():
            h.update(str(p.relative_to(root)).encode())
            h.update(p.read_bytes())
    return h.hexdigest()


class E2E(unittest.TestCase):
    PARAMS = GenParams(seed=7, datasets=6, days=3)

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="slae2e_"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.data = self.tmp / "data"
        self.summary = generate(self.data, self.PARAMS)
        self.cfg = Config(db_path=str(self.tmp / "e.db"), registry_path=str(self.data / "dataset_registry.csv"),
                          landing_dir=str(self.data / "landing"), output_dir=str(self.tmp / "out"),
                          lookback_hours=self.PARAMS.days * 24, log_path=None)
        self.as_of = parse_ts(self.PARAMS.as_of)

    def test_generator_is_deterministic_and_seed_sensitive(self):
        other = self.tmp / "again"
        generate(other, self.PARAMS)
        self.assertEqual(tree_hash(self.data), tree_hash(other))
        diff = self.tmp / "diff"
        generate(diff, dataclasses.replace(self.PARAMS, seed=8))
        self.assertNotEqual(tree_hash(self.data), tree_hash(diff))

    def test_answer_key_is_separate_from_input(self):
        for p in (self.data / "landing").glob("*.csv"):
            header = p.read_text().splitlines()[0]
            self.assertNotIn("expected", header)
            self.assertNotIn("scenario", header)
        self.assertTrue((self.data / "expected" / "expected_labels.csv").exists())

    def test_pipeline_matches_answer_key_and_reconciles(self):
        res = run_pipeline(self.cfg, as_of=self.as_of)
        self.assertEqual(res.status, "SUCCESS")
        store = Store(self.cfg.db_path)
        try:
            cmp = compare_labels(store, self.data / "expected")
            self.assertEqual(cmp["mismatches"], [])
            self.assertEqual((cmp["matched"], cmp["unexpected_partitions"]), (cmp["total"], 0))
            self.assertTrue(cmp["rejects"]["exact_match"], cmp["rejects"])      # every injected defect quarantined, nothing else
            for metric, (want, got) in cmp["counts"].items():
                self.assertEqual(want, got, metric)
            failed = [c for c in reconcile(store) if not c.passed]
            self.assertEqual(failed, [], failed)
        finally:
            store.close()

    def test_all_required_scenarios_are_present_in_generated_data(self):
        sc = self.summary["scenario_counts"]
        for needed in ("ON_TIME", "LATE_ARRIVAL", "LATE_COMPLETION", "MISSING", "FAILED", "OVERDUE",
                       "RETRY_ON_TIME", "NO_ARRIVAL_EVENT", "FUTURE_ARRIVAL", "LOW_ROW_COUNT"):
            self.assertGreater(sc.get(needed, 0), 0, needed)
        for needed in ("DUPLICATE_EVENT", "MISSING_REQUIRED_FIELD", "INVALID_TIMESTAMP", "INVALID_EVENT_TYPE",
                       "UNKNOWN_DATASET", "INVALID_PARTITION_KEY", "INVALID_ROW_COUNT", "MALFORMED_ROW"):
            self.assertGreater(self.summary["reject_reason_counts"].get(needed, 0), 0, needed)
        self.assertGreater(self.summary["totals"]["rows_already_loaded"], 0)

    def test_rerun_after_full_load_is_a_noop(self):
        run_pipeline(self.cfg, as_of=self.as_of)
        s = Store(self.cfg.db_path)
        before = [s.scalar(f"SELECT COUNT(*) FROM {t}") for t in ("load_events", "rejects", "alerts", "partition_sla_status")]
        s.close()
        r = run_pipeline(self.cfg, as_of=self.as_of)
        s = Store(self.cfg.db_path)
        after = [s.scalar(f"SELECT COUNT(*) FROM {t}") for t in ("load_events", "rejects", "alerts", "partition_sla_status")]
        s.close()
        self.assertEqual(before, after)
        self.assertEqual((r.rows_loaded, r.files_skipped), (0, self.summary["totals"] and len(self.summary["per_file"])))

    def test_answer_key_comparison_has_teeth(self):
        """Mutation test: if the evaluator ignored grace periods, the answer key must notice."""
        real = evaluate_mod.classify_partition

        def broken(d, key, events, as_of):
            return real(dataclasses.replace(d, grace_minutes=0), key, events, as_of)

        with mock.patch.object(evaluate_mod, "classify_partition", broken):
            run_pipeline(self.cfg, as_of=self.as_of)
        store = Store(self.cfg.db_path)
        try:
            cmp = compare_labels(store, self.data / "expected")
        finally:
            store.close()
        self.assertGreater(cmp["total"] - cmp["matched"], 0)

    def test_fault_files_from_generator_behave_as_documented(self):
        run_pipeline(self.cfg, as_of=self.as_of)
        faults = self.data / "faults"
        landing = self.tmp / "landing_with_faults"
        shutil.copytree(self.data / "landing", landing)
        for f in faults.glob("*.csv"):
            shutil.copy(f, landing / f.name)
        cfg = dataclasses.replace(self.cfg, landing_dir=str(landing))
        r = run_pipeline(cfg, as_of=self.as_of)
        self.assertEqual(r.status, "PARTIAL_SUCCESS")
        failed = sorted(x["source_file"] for x in r.file_results if x["status"] == "FAILED")
        self.assertEqual(failed, ["bad_schema.csv", "empty_file.csv", "not_utf8.csv"])
        ok = {x["source_file"]: x for x in r.file_results if x["status"] == "LOADED"}
        self.assertEqual(ok["header_only.csv"]["rows_read"], 0)
        self.assertEqual(ok["extra_column.csv"]["rows_already_loaded"], 2)
        self.assertEqual(r.rows_loaded, 0)                                    # nothing new, nothing duplicated


class CliTests(unittest.TestCase):
    def test_generate_run_report_and_exit_codes(self):
        tmp = Path(tempfile.mkdtemp(prefix="slacli_"))
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        data, out, db = str(tmp / "d"), str(tmp / "o"), str(tmp / "o" / "x.db")
        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertEqual(main(["generate", "--out", data, "--datasets", "4", "--days", "2"]), 0)
            self.assertEqual(main(["run", "--data-dir", data, "--db", db, "--output-dir", out,
                                   "--as-of", "2026-01-15T07:40:00Z", "--lookback-hours", "48"]), 0)
            self.assertEqual(main(["report", "--data-dir", data, "--db", db, "--output-dir", out,
                                   "--as-of", "2026-01-15T07:40:00Z"]), 0)
        self.assertIn("validation=PASS", buf.getvalue())
        for f in ("partition_sla_status.csv", "alerts.csv", "dataset_freshness.csv", "rejects.csv",
                  "sla_summary_by_dataset.csv", "validation_report.md", "run_summary.json"):
            self.assertTrue((Path(out) / f).exists(), f)
        shutil.copy(Path(data) / "faults" / "bad_schema.csv", Path(data) / "landing" / "bad_schema.csv")
        with redirect_stdout(io.StringIO()):
            code = main(["run", "--data-dir", data, "--db", db, "--output-dir", out,
                         "--as-of", "2026-01-15T07:40:00Z", "--lookback-hours", "48"])
        self.assertEqual(code, 2)                                            # PARTIAL_SUCCESS
        self.assertEqual(json.loads((Path(out) / "run_summary.json").read_text())["status"], "PARTIAL_SUCCESS")


if __name__ == "__main__":
    unittest.main()
