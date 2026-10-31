import unittest

from sla_monitor.schedule import DatasetDef
from sla_monitor.validation import RecordError, validate_event_row, validate_registry_row

DS = {"ds_d": DatasetDef("ds_d", "d", "o", "daily", 120, 30, 90, 1, "low"),
      "ds_h": DatasetDef("ds_h", "h", "o", "hourly", 10, 15, 40, 1, "low")}


def row(**kw):
    base = {"dataset_id": "ds_d", "partition_key": "2026-01-15", "event_type": "ARRIVED",
            "event_ts": "2026-01-15T02:10:00Z", "row_count": ""}
    base.update(kw)
    return base


def code(r, ds=DS):
    with_err = None
    try:
        validate_event_row(r, ds)
    except RecordError as e:
        with_err = e.code
    return with_err


class EventValidationTests(unittest.TestCase):
    def test_valid_and_normalised(self):
        ev = validate_event_row(row(event_type="arrived", event_ts="2026-01-15T07:40:00+05:30"), DS)
        self.assertEqual((ev["event_type"], ev["event_ts"], ev["row_count"]), ("ARRIVED", "2026-01-15T02:10:00Z", None))

    def test_same_instant_same_event_id_different_instant_different(self):
        a = validate_event_row(row(), DS)["event_id"]
        self.assertEqual(a, validate_event_row(row(event_ts="2026-01-15T07:40:00+05:30"), DS)["event_id"])
        self.assertNotEqual(a, validate_event_row(row(event_ts="2026-01-15T02:10:01Z"), DS)["event_id"])
        self.assertTrue(a.startswith("evt_") and len(a) == 20)

    def test_missing_required_each_column(self):
        for col in ("dataset_id", "partition_key", "event_type", "event_ts"):
            self.assertEqual(code(row(**{col: ""})), "MISSING_REQUIRED_FIELD", col)
            self.assertEqual(code(row(**{col: "   "})), "MISSING_REQUIRED_FIELD", col)

    def test_row_count_required_only_for_completed(self):
        self.assertIsNone(code(row(event_type="ARRIVED", row_count="")))
        self.assertEqual(code(row(event_type="COMPLETED", row_count="")), "MISSING_REQUIRED_FIELD")

    def test_row_count_boundaries(self):
        self.assertIsNone(code(row(event_type="COMPLETED", row_count="0")))          # zero is valid
        for bad in ("-1", "abc", "12.5", "1e3", "1,000"):
            self.assertEqual(code(row(event_type="COMPLETED", row_count=bad)), "INVALID_ROW_COUNT", bad)

    def test_event_type_unknown_dataset_timestamp_partition(self):
        self.assertEqual(code(row(event_type="STARTED")), "INVALID_EVENT_TYPE")
        self.assertEqual(code(row(dataset_id="ds_nope")), "UNKNOWN_DATASET")
        for bad in ("2026-01-15T02:10:00", "not-a-date", "2026-13-45T99:00:00Z"):
            self.assertEqual(code(row(event_ts=bad)), "INVALID_TIMESTAMP", bad)
        self.assertEqual(code(row(partition_key="2026-01-15T05")), "INVALID_PARTITION_KEY")   # hourly key on daily ds
        self.assertEqual(code(row(partition_key="2026-02-30")), "INVALID_PARTITION_KEY")
        self.assertEqual(code(row(dataset_id="ds_h", partition_key="2026-01-15")), "INVALID_PARTITION_KEY")


def reg(**kw):
    base = {"dataset_id": "ds_x", "dataset_name": "X", "owner": "o", "frequency": "daily",
            "expected_offset_minutes": "120", "grace_minutes": "30", "completion_sla_minutes": "90",
            "min_rows": "10", "severity": "high"}
    base.update(kw)
    return base


def rcode(r):
    try:
        validate_registry_row(r)
    except RecordError as e:
        return e.code


class RegistryValidationTests(unittest.TestCase):
    def test_valid(self):
        d = validate_registry_row(reg(frequency="HOURLY", expected_offset_minutes="59", severity="High"))
        self.assertEqual((d.frequency, d.severity, d.expected_offset_minutes), ("hourly", "high", 59))

    def test_rules(self):
        self.assertEqual(rcode(reg(owner="")), "MISSING_REQUIRED_FIELD")
        self.assertEqual(rcode(reg(dataset_id="Bad-Id")), "INVALID_DATASET_ID")
        self.assertEqual(rcode(reg(frequency="weekly")), "INVALID_FREQUENCY")
        self.assertEqual(rcode(reg(severity="urgent")), "INVALID_SEVERITY")
        self.assertEqual(rcode(reg(grace_minutes="x")), "INVALID_NUMBER")
        self.assertEqual(rcode(reg(min_rows="-1")), "INVALID_NUMBER")
        self.assertEqual(rcode(reg(frequency="hourly", expected_offset_minutes="60")), "INVALID_SLA_DEFINITION")
        self.assertEqual(rcode(reg(expected_offset_minutes="1440")), "INVALID_SLA_DEFINITION")
        self.assertEqual(rcode(reg(grace_minutes="60", completion_sla_minutes="30")), "INVALID_SLA_DEFINITION")
        self.assertIsNone(rcode(reg(grace_minutes="30", completion_sla_minutes="30")))      # equal is allowed


if __name__ == "__main__":
    unittest.main()
