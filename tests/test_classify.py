"""Pure SLA-rule tests (no I/O). Daily partition 2026-01-15: expected 02:00, arrival deadline 02:30,
completion deadline 03:30."""
import unittest

from sla_monitor.evaluate import alerts_for, classify_partition
from sla_monitor.schedule import DatasetDef
from sla_monitor.timeutil import parse_ts

D = DatasetDef("ds_d", "d", "o", "daily", 120, 30, 90, 100, "high")
K = "2026-01-15"
AFTER = parse_ts("2026-01-15T12:00:00Z")          # well after every deadline


def z(hms):
    return f"2026-01-15T{hms}Z"


def cl(events, as_of=AFTER):
    return classify_partition(D, K, events, as_of)


class ClassifyTests(unittest.TestCase):
    def test_on_time(self):
        r = cl([("ARRIVED", z("02:10:00"), None), ("COMPLETED", z("03:00:00"), 500)])
        self.assertEqual((r.arrival_status, r.completion_status, r.sla_status), ("ON_TIME", "ON_TIME", "MET"))
        self.assertEqual(r.arrival_delay_minutes, 10.0)
        self.assertEqual(r.completion_latency_minutes, 60.0)

    def test_arrival_boundary_inclusive_then_one_second_late(self):
        on = cl([("ARRIVED", z("02:30:00"), None), ("COMPLETED", z("03:00:00"), 500)])
        late = cl([("ARRIVED", z("02:30:01"), None), ("COMPLETED", z("03:00:00"), 500)])
        self.assertEqual(on.arrival_status, "ON_TIME")
        self.assertEqual((late.arrival_status, late.sla_status), ("LATE", "BREACHED"))

    def test_completion_boundary_inclusive_then_one_second_late(self):
        on = cl([("ARRIVED", z("02:10:00"), None), ("COMPLETED", z("03:30:00"), 500)])
        late = cl([("ARRIVED", z("02:10:00"), None), ("COMPLETED", z("03:30:01"), 500)])
        self.assertEqual(on.completion_status, "ON_TIME")
        self.assertEqual((late.completion_status, late.sla_status), ("LATE", "BREACHED"))

    def test_pending_then_missing_at_deadline_boundary(self):
        self.assertEqual(cl([], parse_ts(z("02:30:00"))).arrival_status, "PENDING")     # as_of == deadline: not yet missing
        r = cl([], parse_ts(z("02:30:01")))
        self.assertEqual((r.arrival_status, r.completion_status, r.sla_status), ("MISSING", "PENDING", "BREACHED"))
        r = cl([], parse_ts(z("03:30:01")))
        self.assertEqual((r.arrival_status, r.completion_status), ("MISSING", "OVERDUE"))

    def test_arrived_but_never_completed(self):
        pend = cl([("ARRIVED", z("02:10:00"), None)], parse_ts(z("03:30:00")))
        self.assertEqual((pend.completion_status, pend.sla_status), ("PENDING", "PENDING"))
        over = cl([("ARRIVED", z("02:10:00"), None)], parse_ts(z("03:30:01")))
        self.assertEqual((over.completion_status, over.sla_status), ("OVERDUE", "BREACHED"))

    def test_completion_without_arrival_event_implies_arrival(self):          # domain edge case 1
        r = cl([("COMPLETED", z("02:20:00"), 500)])
        self.assertTrue(r.arrival_inferred)
        self.assertEqual((r.arrival_status, r.sla_status), ("ON_TIME", "MET"))

    def test_out_of_order_events_give_same_result(self):                      # domain edge case 2
        a = [("ARRIVED", z("02:10:00"), None), ("FAILED", z("02:20:00"), None), ("COMPLETED", z("03:00:00"), 500)]
        b = list(reversed(a))
        self.assertEqual(cl(a), cl(b))

    def test_failed_then_retry_completed_is_ok_but_failed_alone_is_not(self):  # domain edge case 3
        ok = cl([("ARRIVED", z("02:10:00"), None), ("FAILED", z("02:15:00"), None), ("COMPLETED", z("03:00:00"), 500)])
        self.assertEqual((ok.completion_status, ok.sla_status), ("ON_TIME", "MET"))
        bad = cl([("ARRIVED", z("02:10:00"), None), ("FAILED", z("02:15:00"), None)])
        self.assertEqual((bad.completion_status, bad.sla_status), ("FAILED", "BREACHED"))
        early = cl([("ARRIVED", z("02:10:00"), None), ("FAILED", z("02:15:00"), None)], parse_ts(z("02:20:00")))
        self.assertEqual((early.completion_status, early.sla_status), ("FAILED", "PENDING"))   # still time to retry

    def test_low_row_count_boundary(self):                                     # domain edge case 4
        self.assertTrue(cl([("ARRIVED", z("02:10:00"), None), ("COMPLETED", z("03:00:00"), 99)]).low_row_count)
        self.assertFalse(cl([("ARRIVED", z("02:10:00"), None), ("COMPLETED", z("03:00:00"), 100)]).low_row_count)
        self.assertTrue(cl([("ARRIVED", z("02:10:00"), None), ("COMPLETED", z("03:00:00"), 0)]).low_row_count)

    def test_latest_completion_row_count_wins_but_first_completion_sets_sla(self):
        r = cl([("ARRIVED", z("02:10:00"), None), ("COMPLETED", z("03:00:00"), 5), ("COMPLETED", z("09:00:00"), 500)])
        self.assertEqual((r.completion_status, r.row_count, r.low_row_count), ("ON_TIME", 500, False))

    def test_completion_before_arrival_is_flagged_not_hidden(self):
        r = cl([("ARRIVED", z("02:40:00"), None), ("COMPLETED", z("02:35:00"), 500)])
        self.assertTrue(r.timestamp_order_anomaly)

    def test_duplicate_arrivals_use_earliest(self):
        r = cl([("ARRIVED", z("02:45:00"), None), ("ARRIVED", z("02:10:00"), None), ("COMPLETED", z("03:00:00"), 500)])
        self.assertEqual(r.arrival_status, "ON_TIME")


class AlertRuleTests(unittest.TestCase):
    def types(self, events, as_of=AFTER):
        return sorted(t for t, _ in alerts_for(D, cl(events, as_of)))

    def test_missing_suppresses_completion_alerts(self):
        self.assertEqual(self.types([]), ["MISSING_LOAD"])

    def test_each_type(self):
        self.assertEqual(self.types([("ARRIVED", z("02:40:00"), None), ("COMPLETED", z("03:00:00"), 500)]), ["LATE_ARRIVAL"])
        self.assertEqual(self.types([("ARRIVED", z("02:10:00"), None), ("COMPLETED", z("04:00:00"), 500)]), ["LATE_COMPLETION"])
        self.assertEqual(self.types([("ARRIVED", z("02:10:00"), None)]), ["INCOMPLETE_LOAD"])
        self.assertEqual(self.types([("ARRIVED", z("02:10:00"), None), ("FAILED", z("02:12:00"), None)]), ["LOAD_FAILED"])
        self.assertEqual(self.types([("ARRIVED", z("02:10:00"), None), ("COMPLETED", z("03:00:00"), 1)]), ["LOW_ROW_COUNT"])

    def test_healthy_partition_has_no_alerts(self):
        self.assertEqual(self.types([("ARRIVED", z("02:10:00"), None), ("COMPLETED", z("03:00:00"), 500)]), [])


if __name__ == "__main__":
    unittest.main()
