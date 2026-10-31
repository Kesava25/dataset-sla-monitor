import os
import sqlite3
from pathlib import Path

from sla_monitor.timeutil import parse_ts
from tests.helpers import DEFAULT_REGISTRY, EVENT_HEADER, T, PipelineCase, ev, write_csv

GOOD = [ev("ds_daily", "2026-01-15", "ARRIVED", "2026-01-15T02:10:00Z"),
        ev("ds_daily", "2026-01-15", "COMPLETED", "2026-01-15T03:00:00Z", 500)]


class HappyAndEmptyTests(PipelineCase):
    def test_happy_path(self):
        self.events("load_events_a.csv", GOOD)
        r = self.run_pipe()
        self.assertEqual((r.status, r.exit_code), ("SUCCESS", 0))
        self.assertEqual((r.rows_read, r.rows_loaded, r.rows_rejected, r.rows_already_loaded), (2, 2, 0, 0))
        self.assertEqual(self.status("ds_daily", "2026-01-15"), ("ON_TIME", "ON_TIME", "MET"))
        row = self.q("SELECT status,rows_read,rows_loaded,files_loaded,duration_seconds,failure_reason FROM pipeline_runs")[0]
        self.assertEqual(row[:4], ("SUCCESS", 2, 2, 1))
        self.assertIsNotNone(row[4])
        self.assertTrue((Path(self.cfg.output_dir) / "run_summary.json").exists())

    def test_empty_batch_header_only(self):
        self.events("load_events_a.csv", [])
        r = self.run_pipe()
        self.assertEqual((r.status, r.rows_read, r.files_loaded), ("SUCCESS", 0, 1))
        self.assertIn("empty batch", self.one("SELECT warnings FROM file_ingest_log"))
        self.assertEqual(self.one("SELECT COUNT(*) FROM load_events"), 0)
        self.assertEqual(self.status("ds_daily", "2026-01-15"), ("MISSING", "OVERDUE", "BREACHED"))   # nothing arrived

    def test_empty_directory_is_success_with_zero_files(self):
        r = self.run_pipe()
        self.assertEqual((r.status, r.files_seen, r.rows_read), ("SUCCESS", 0, 0))
        self.assertGreater(r.partitions_evaluated, 0)          # still evaluates: absence of data IS the signal
        self.assertGreater(r.alerts_open, 0)

    def test_zero_byte_and_non_utf8_files_fail_cleanly(self):
        (self.tmp / "landing" / "load_events_empty.csv").write_bytes(b"")
        (self.tmp / "landing" / "load_events_bin.csv").write_bytes(b"\xff\xfe\x00junk\x80")
        r = self.run_pipe()
        self.assertEqual((r.status, r.files_failed), ("FAILED", 2))
        codes = {c for (c,) in self.q("SELECT reason_code FROM rejects WHERE entity='file'")}
        self.assertEqual(codes, {"EMPTY_FILE", "NOT_UTF8_TEXT"})

    def test_bom_blank_lines_and_header_case_tolerated(self):
        p = self.tmp / "landing" / "load_events_a.csv"
        p.write_bytes("\ufeffDataset_ID,partition_key,EVENT_TYPE,event_ts,row_count\n\n"
                      "ds_daily,2026-01-15,ARRIVED,2026-01-15T02:10:00Z,\n\n".encode("utf-8"))
        r = self.run_pipe()
        self.assertEqual((r.status, r.rows_read, r.rows_loaded), ("SUCCESS", 1, 1))


class QualityTests(PipelineCase):
    def test_malformed_missing_invalid_records_are_quarantined_and_traceable(self):
        rows = [GOOD[0], GOOD[1],
                ["ds_daily", "2026-01-15", "ARRIVED"],                                         # line 4: too few fields
                ev("ds_daily", "2026-01-15", "ARRIVED", "2026-01-15T02:10:00Z") + ["x"],       # line 5: too many
                ev("", "2026-01-15", "ARRIVED", "2026-01-15T02:10:00Z"),                       # line 6: null key
                ev("ds_daily", "2026-01-15", "COMPLETED", "2026-01-15T03:00:00Z", ""),         # line 7: null row_count
                ev("ds_nope", "2026-01-15", "ARRIVED", "2026-01-15T02:10:00Z"),                # line 8: bad reference
                ev("ds_daily", "2026-01-15", "ARRIVED", "2026-01-15 02:10"),                   # line 9: bad timestamp
                ev("ds_daily", "2026-01-15", "COMPLETED", "2026-01-15T03:01:00Z", -3)]         # line 10: bad row_count
        self.events("load_events_a.csv", rows)
        r = self.run_pipe()
        self.assertEqual((r.rows_read, r.rows_loaded, r.rows_rejected), (9, 2, 7))
        got = self.q("SELECT source_line,reason_code,source_file FROM rejects ORDER BY source_line")
        self.assertEqual([(l, c) for l, c, _ in got], [
            (4, "MALFORMED_ROW"), (5, "MALFORMED_ROW"), (6, "MISSING_REQUIRED_FIELD"), (7, "MISSING_REQUIRED_FIELD"),
            (8, "UNKNOWN_DATASET"), (9, "INVALID_TIMESTAMP"), (10, "INVALID_ROW_COUNT")])
        self.assertTrue(all(f == "load_events_a.csv" for *_, f in got))
        self.assertIn("ds_nope", self.one("SELECT raw_record FROM rejects WHERE source_line=8"))   # raw record preserved
        self.assertEqual(self.one("SELECT COUNT(*) FROM load_events"), 2)                         # good rows unaffected

    def test_duplicates_within_file_first_wins_conflict_is_explained(self):
        rows = GOOD + [GOOD[0], ev("ds_daily", "2026-01-15", "COMPLETED", "2026-01-15T03:00:00Z", 999)]
        self.events("load_events_a.csv", rows)
        r = self.run_pipe()
        self.assertEqual((r.rows_loaded, r.rows_rejected), (2, 2))
        d = self.q("SELECT source_line,reason_detail FROM rejects WHERE reason_code='DUPLICATE_EVENT' ORDER BY 1")
        self.assertEqual([x[0] for x in d], [4, 5])
        self.assertIn("same row_count", d[0][1])
        self.assertIn("CONFLICTING", d[1][1])
        self.assertEqual(self.one("SELECT row_count FROM load_events WHERE event_type='COMPLETED'"), 500)   # first kept

    def test_timezone_offset_duplicates_collapse(self):
        rows = [GOOD[0], ev("ds_daily", "2026-01-15", "ARRIVED", "2026-01-15T07:40:00+05:30")]   # same instant
        self.events("load_events_a.csv", rows)
        r = self.run_pipe()
        self.assertEqual((r.rows_loaded, r.rows_rejected), (1, 1))

    def test_unexpected_extra_column_is_tolerated_with_warning(self):
        self.events("load_events_a.csv", [row + ["v2"] for row in GOOD], header=EVENT_HEADER + ["producer_version"])
        r = self.run_pipe()
        self.assertEqual((r.status, r.rows_loaded), ("SUCCESS", 2))
        self.assertIn("producer_version", self.one("SELECT warnings FROM file_ingest_log"))


class SchemaChangeAndPartialFailureTests(PipelineCase):
    def test_schema_change_fails_that_file_only_then_recovers_when_fixed(self):
        self.events("load_events_a.csv", GOOD)
        bad = ["dataset_id", "partition_key", "event_type", "event_time", "row_count"]           # event_ts renamed
        self.events("load_events_b.csv", [ev("ds_hourly", "2026-01-15T05", "ARRIVED", "2026-01-15T05:12:00Z")], header=bad)
        r = self.run_pipe()
        self.assertEqual((r.status, r.exit_code, r.files_loaded, r.files_failed), ("PARTIAL_SUCCESS", 2, 1, 1))
        self.assertIn("event_ts", self.one("SELECT error FROM file_ingest_log WHERE status='FAILED'"))
        self.assertEqual(self.one("SELECT reason_code FROM rejects WHERE entity='file'"), "SCHEMA_MISMATCH")
        self.assertEqual(self.one("SELECT COUNT(*) FROM load_events"), 2)
        # producer fixes the file -> next run loads it (different sha) and the run is clean
        self.events("load_events_b.csv", [ev("ds_hourly", "2026-01-15T05", "ARRIVED", "2026-01-15T05:12:00Z")])
        r2 = self.run_pipe()
        self.assertEqual((r2.status, r2.files_loaded, r2.files_skipped, r2.rows_loaded), ("SUCCESS", 1, 1, 1))

    def test_failed_file_is_retried_every_run_until_fixed(self):
        self.events("load_events_b.csv", GOOD, header=["a", "b"])
        for _ in range(2):
            self.assertEqual(self.run_pipe().status, "FAILED")
        self.assertEqual(self.one("SELECT COUNT(*) FROM file_ingest_log WHERE status='FAILED'"), 1)   # one row, not two
        self.assertEqual(self.one("SELECT COUNT(*) FROM rejects WHERE entity='file'"), 1)

    def test_crash_mid_file_rolls_back_then_retry_loads_everything(self):
        self.events("load_events_a.csv", GOOD)
        self.events("load_events_b.csv", [ev("ds_hourly", "2026-01-15T05", "ARRIVED", "2026-01-15T05:12:00Z"),
                                          ["bad"], ev("ds_hourly", "2026-01-15T05", "COMPLETED", "2026-01-15T05:40:00Z", 50)])

        def boom(name, stage):
            if name == "load_events_b.csv":
                raise RuntimeError("simulated crash before commit")

        r = self.run_pipe(fault_hook=boom)
        self.assertEqual((r.status, r.files_loaded, r.files_failed), ("PARTIAL_SUCCESS", 1, 1))
        self.assertEqual(self.one("SELECT COUNT(*) FROM load_events WHERE source_file='load_events_b.csv'"), 0)   # atomic
        self.assertEqual(self.one("SELECT COUNT(*) FROM rejects WHERE source_file='load_events_b.csv' AND entity='event'"), 0)
        r2 = self.run_pipe()                                     # retry, no fault
        self.assertEqual((r2.status, r2.files_loaded, r2.files_skipped, r2.rows_loaded, r2.rows_rejected), ("SUCCESS", 1, 1, 2, 1))
        self.assertEqual(self.one("SELECT COUNT(*) FROM load_events"), 4)

    def test_killed_run_is_marked_abandoned_by_next_run(self):
        self.events("load_events_a.csv", GOOD)
        self.run_pipe()
        con = sqlite3.connect(self.cfg.db_path)
        con.execute("INSERT INTO pipeline_runs(run_id,started_at,status) VALUES ('run_dead','2026-01-01T00:00:00Z','RUNNING')")
        con.commit(); con.close()
        self.run_pipe()
        self.assertEqual(self.one("SELECT status FROM pipeline_runs WHERE run_id='run_dead'"), "ABANDONED")

    def test_missing_registry_fails_run_with_reason(self):
        os.remove(self.cfg.registry_path)
        self.events("load_events_a.csv", GOOD)
        r = self.run_pipe()
        self.assertEqual((r.status, r.exit_code), ("FAILED", 1))
        self.assertIn("no dataset registry", r.failure_reason)
        self.assertEqual(self.one("SELECT status FROM pipeline_runs"), "FAILED")

    def test_registry_removed_later_uses_stored_definitions(self):
        self.events("load_events_a.csv", GOOD)
        self.run_pipe()
        os.remove(self.cfg.registry_path)
        r = self.run_pipe()
        self.assertEqual(r.status, "SUCCESS")


class IdempotencyTests(PipelineCase):
    def messy_input(self):
        self.events("load_events_a.csv", GOOD + [["bad"], GOOD[0]])
        self.events("load_events_b.csv", [GOOD[1], ev("ds_hourly", "2026-01-15T05", "ARRIVED", "2026-01-15T05:12:00Z")])

    def test_rerun_same_input_changes_nothing(self):
        self.messy_input()
        first = self.run_pipe()
        snap1 = self.snapshot()
        second = self.run_pipe()
        self.assertEqual(snap1, self.snapshot())
        self.assertEqual((second.files_skipped, second.rows_loaded, second.rows_read), (2, 0, 0))
        self.assertEqual(first.rows_loaded, 3)
        self.assertEqual(self.one("SELECT COUNT(*) FROM pipeline_runs"), 2)       # runs are history, never overwritten

    def test_event_replayed_in_another_file_is_not_duplicated_and_not_rejected(self):
        self.messy_input()
        r = self.run_pipe()
        self.assertEqual(r.rows_already_loaded, 1)                                # GOOD[1] again in file b
        self.assertEqual(self.one("SELECT COUNT(*) FROM load_events"), 3)
        self.assertEqual(self.one("SELECT source_file FROM load_events WHERE event_type='COMPLETED'"), "load_events_a.csv")

    def test_changed_file_with_same_name_reloads_only_new_events(self):
        self.events("load_events_a.csv", GOOD[:1])
        self.run_pipe()
        self.events("load_events_a.csv", GOOD)                                    # producer appended a row
        r = self.run_pipe()
        self.assertEqual((r.files_loaded, r.rows_loaded, r.rows_already_loaded), (1, 1, 1))
        self.assertEqual(self.one("SELECT COUNT(*) FROM load_events"), 2)

    def test_evaluation_is_deterministic_for_same_as_of(self):
        self.messy_input()
        self.run_pipe()
        a = self.snapshot()
        self.run_pipe(as_of=T)
        self.assertEqual(a, self.snapshot())

    def test_input_file_order_does_not_change_results(self):
        self.messy_input()
        self.run_pipe()
        a = self.snapshot()["status_rows"]
        os.remove(self.cfg.db_path)
        os.rename(self.tmp / "landing" / "load_events_a.csv", self.tmp / "landing" / "load_events_z.csv")   # reorder
        self.run_pipe()
        self.assertEqual(a, self.snapshot()["status_rows"])


class RegistryHistoryTests(PipelineCase):
    def test_definition_change_appends_version_and_applies_going_forward(self):
        self.events("load_events_a.csv", [GOOD[0], ev("ds_daily", "2026-01-15", "ARRIVED", "2026-01-15T02:20:00Z"), GOOD[1]])
        self.run_pipe()
        self.assertEqual(self.one("SELECT COUNT(*) FROM dataset_registry_history WHERE dataset_id='ds_daily'"), 1)
        self.run_pipe()                                                           # unchanged -> no new version
        self.assertEqual(self.one("SELECT COUNT(*) FROM dataset_registry_history WHERE dataset_id='ds_daily'"), 1)
        tighter = [list(DEFAULT_REGISTRY[0]), DEFAULT_REGISTRY[1]]
        tighter[0][5] = "5"                                                       # grace 30 -> 5 minutes
        self.write_registry(tighter)
        self.run_pipe()
        self.assertEqual(self.q("SELECT version,grace_minutes FROM dataset_registry_history WHERE dataset_id='ds_daily' ORDER BY 1"),
                         [(1, 30), (2, 5)])
        self.assertEqual(self.status("ds_daily", "2026-01-15")[0], "LATE")        # 02:10 now exceeds 02:05

    def test_invalid_and_duplicate_registry_rows_rejected_valid_ones_kept(self):
        self.write_registry(DEFAULT_REGISTRY + [["ds_x", "X", "o", "weekly", "1", "1", "2", "1", "low"],
                                                ["ds_daily", "dup", "o", "daily", "1", "1", "2", "1", "low"]])
        r = self.run_pipe()
        self.assertEqual((r.status, r.registry_rows_rejected), ("SUCCESS", 2))
        self.assertEqual(self.one("SELECT COUNT(*) FROM dataset_registry"), 2)
        self.assertEqual(sorted(c for (c,) in self.q("SELECT reason_code FROM rejects WHERE entity='registry'")),
                         ["DUPLICATE_DATASET_ID", "INVALID_FREQUENCY"])


class AlertAndFreshnessTests(PipelineCase):
    def alerts(self):
        return self.q("SELECT alert_type,status,partition_key FROM alerts WHERE dataset_id='ds_daily' ORDER BY 1,3")

    def test_alert_lifecycle_open_then_resolved_and_history_kept(self):
        self.events("load_events_a.csv", [])
        self.run_pipe(as_of=parse_ts("2026-01-15T02:30:00Z"))                      # exactly at deadline: still pending
        self.assertEqual(self.status("ds_daily", "2026-01-15")[0], "PENDING")
        self.assertNotIn(("MISSING_LOAD", "OPEN", "2026-01-15"), self.alerts())
        self.run_pipe(as_of=parse_ts("2026-01-15T04:00:00Z"))
        self.assertIn(("MISSING_LOAD", "OPEN", "2026-01-15"), self.alerts())
        first = self.one("SELECT first_detected_as_of FROM alerts WHERE alert_type='MISSING_LOAD' AND partition_key='2026-01-15' "
                         "AND dataset_id='ds_daily'")
        self.assertEqual(first, "2026-01-15T04:00:00Z")
        # the data finally lands, late
        self.events("load_events_a.csv", [ev("ds_daily", "2026-01-15", "ARRIVED", "2026-01-15T05:00:00Z"),
                                          ev("ds_daily", "2026-01-15", "COMPLETED", "2026-01-15T05:10:00Z", 500)])
        r = self.run_pipe(as_of=parse_ts("2026-01-15T06:00:00Z"))
        self.assertIn(("MISSING_LOAD", "RESOLVED", "2026-01-15"), self.alerts())
        self.assertIn(("LATE_ARRIVAL", "OPEN", "2026-01-15"), self.alerts())
        self.assertIn(("LATE_COMPLETION", "OPEN", "2026-01-15"), self.alerts())
        self.assertGreaterEqual(r.alerts_resolved, 1)
        self.assertEqual(self.one("SELECT resolved_as_of FROM alerts WHERE alert_type='MISSING_LOAD' AND dataset_id='ds_daily' "
                                  "AND partition_key='2026-01-15'"), "2026-01-15T06:00:00Z")
        self.assertEqual(self.one("SELECT first_detected_as_of FROM alerts WHERE alert_type='MISSING_LOAD' AND dataset_id='ds_daily' "
                                  "AND partition_key='2026-01-15'"), first)        # history preserved

    def test_future_events_are_invisible_to_earlier_as_of(self):                  # late/future data edge case
        self.events("load_events_a.csv", [ev("ds_daily", "2026-01-15", "ARRIVED", "2026-01-15T09:00:00Z"),
                                          ev("ds_daily", "2026-01-15", "COMPLETED", "2026-01-15T09:05:00Z", 500)])
        self.run_pipe(as_of=parse_ts("2026-01-15T08:00:00Z"))
        self.assertEqual(self.status("ds_daily", "2026-01-15")[0], "MISSING")
        self.run_pipe(as_of=parse_ts("2026-01-15T10:00:00Z"))                      # replay later: same stored data
        self.assertEqual(self.status("ds_daily", "2026-01-15")[0], "LATE")

    def test_aged_out_open_alert_is_not_silently_resolved(self):
        self.run_pipe(as_of=parse_ts("2026-01-15T04:00:00Z"))
        self.assertIn(("MISSING_LOAD", "OPEN", "2026-01-15"), self.alerts())
        self.cfg.lookback_hours = 24
        self.run_pipe(as_of=parse_ts("2026-01-20T00:00:00Z"))                      # 2026-01-15 is outside the window now
        self.assertIn(("MISSING_LOAD", "OPEN", "2026-01-15"), self.alerts())

    def test_freshness_fresh_stale_never_loaded(self):
        self.events("load_events_a.csv", GOOD)
        self.run_pipe(as_of=parse_ts("2026-01-15T05:00:00Z"))
        f = dict(self.q("SELECT dataset_id,freshness_status FROM dataset_freshness"))
        self.assertEqual(f, {"ds_daily": "FRESH", "ds_hourly": "NEVER_LOADED"})
        self.run_pipe(as_of=parse_ts("2026-01-17T05:00:00Z"))                      # daily threshold 1560 min; 50h old
        f = dict(self.q("SELECT dataset_id,freshness_status FROM dataset_freshness"))
        self.assertEqual(f["ds_daily"], "STALE")
        self.assertIn(("STALE_DATASET", "OPEN"), [(a, s) for a, s, _ in self.alerts()])
        self.assertEqual(self.one("SELECT freshness_minutes FROM dataset_freshness WHERE dataset_id='ds_daily'"), 3000.0)  # 50 h

    def test_hourly_partition_across_midnight(self):
        self.events("load_events_a.csv", [ev("ds_hourly", "2026-01-14T23", "ARRIVED", "2026-01-14T23:12:00Z"),
                                          ev("ds_hourly", "2026-01-14T23", "COMPLETED", "2026-01-15T00:05:00Z", 50)])
        self.run_pipe(as_of=parse_ts("2026-01-15T02:00:00Z"))
        # completion deadline = 23:10 + 40min = 23:50 -> 00:05 next day is LATE
        self.assertEqual(self.status("ds_hourly", "2026-01-14T23"), ("ON_TIME", "LATE", "BREACHED"))

    def test_low_row_count_alert_raised(self):
        self.events("load_events_a.csv", [GOOD[0], ev("ds_daily", "2026-01-15", "COMPLETED", "2026-01-15T03:00:00Z", 3)])
        self.run_pipe()
        self.assertIn(("LOW_ROW_COUNT", "OPEN", "2026-01-15"), self.alerts())
        self.assertEqual(self.status("ds_daily", "2026-01-15"), ("ON_TIME", "ON_TIME", "MET"))      # SLA met, quality flagged
