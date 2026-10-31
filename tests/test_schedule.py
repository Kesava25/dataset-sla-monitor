import unittest
from datetime import timedelta

from sla_monitor.schedule import DatasetDef
from sla_monitor.timeutil import fmt_ts, parse_ts

D = DatasetDef("ds_d", "d", "o", "daily", 120, 30, 90, 1, "low")
H = DatasetDef("ds_h", "h", "o", "hourly", 10, 15, 40, 1, "low")


class ScheduleTests(unittest.TestCase):
    def test_daily_deadlines(self):
        self.assertEqual(fmt_ts(D.expected_arrival("2026-01-15")), "2026-01-15T02:00:00Z")
        self.assertEqual(fmt_ts(D.arrival_deadline("2026-01-15")), "2026-01-15T02:30:00Z")
        self.assertEqual(fmt_ts(D.completion_deadline("2026-01-15")), "2026-01-15T03:30:00Z")

    def test_hourly_deadlines_cross_midnight(self):
        # 23:00 + 50 min offset + 80 min SLA = 01:10 next day (crosses day and month boundary)
        h = DatasetDef("ds_h", "h", "o", "hourly", 50, 15, 80, 1, "low")
        self.assertEqual(fmt_ts(h.completion_deadline("2026-01-31T23")), "2026-02-01T01:10:00Z")

    def test_invalid_partition_keys(self):
        for bad in ("2026-02-30", "2026-01-15T05", "15-01-2026", ""):
            with self.assertRaises(ValueError):
                D.partition_start(bad)
        for bad in ("2026-01-15", "2026-01-15T24", "2026-01-15T5"):
            with self.assertRaises(ValueError):
                H.partition_start(bad)

    def test_window_hourly_lower_bound_exclusive_upper_inclusive(self):
        keys = H.partition_keys(parse_ts("2026-01-15T12:00:00Z"), 3)      # expected 09:10,10:10,11:10
        self.assertEqual(keys, ["2026-01-15T09", "2026-01-15T10", "2026-01-15T11"])
        # as_of exactly on an expected arrival: that partition is IN, the one exactly lookback ago is OUT
        keys = H.partition_keys(parse_ts("2026-01-15T12:10:00Z"), 3)
        self.assertEqual(keys, ["2026-01-15T10", "2026-01-15T11", "2026-01-15T12"])

    def test_window_month_and_year_rollover(self):
        d0 = DatasetDef("ds_d", "d", "o", "daily", 0, 10, 30, 1, "low")
        self.assertEqual(d0.partition_keys(parse_ts("2026-03-01T00:00:00Z"), 48), ["2026-02-28", "2026-03-01"])
        self.assertEqual(d0.partition_keys(parse_ts("2026-01-01T06:00:00Z"), 48), ["2025-12-31", "2026-01-01"])

    def test_no_future_partitions(self):
        keys = D.partition_keys(parse_ts("2026-01-15T01:59:59Z"), 72)
        self.assertNotIn("2026-01-15", keys)
        self.assertEqual(keys[-1], "2026-01-14")

    def test_max_staleness(self):
        self.assertEqual(D.max_staleness_minutes(), 1440 + 30 + 90)
        self.assertEqual(H.max_staleness_minutes(), 60 + 15 + 40)


class TimeTests(unittest.TestCase):
    def test_parse_variants_same_instant(self):
        a = parse_ts("2026-01-15T02:00:00Z")
        self.assertEqual(a, parse_ts("2026-01-15T07:30:00+05:30"))
        self.assertEqual(a, parse_ts("2026-01-15T02:00:00.999+00:00"))     # sub-second truncated

    def test_rejects_naive_and_garbage(self):
        for bad in ("2026-01-15T02:00:00", "", "   ", "yesterday", "2026-13-01T00:00:00Z", None):
            with self.assertRaises(ValueError, msg=repr(bad)):
                parse_ts(bad)


if __name__ == "__main__":
    unittest.main()
