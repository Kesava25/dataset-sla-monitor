"""SLA schedule maths: which partitions are expected, and by when.

Definitions (all UTC):
  partition_key        daily  -> 'YYYY-MM-DD'      hourly -> 'YYYY-MM-DDTHH'
  expected_arrival     partition start + expected_offset_minutes
  arrival_deadline     expected_arrival + grace_minutes          (arrival on/before this is ON_TIME)
  completion_deadline  expected_arrival + completion_sla_minutes (completion on/before is ON_TIME)
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from .timeutil import UTC

FREQUENCIES = ("hourly", "daily")
SEVERITIES = ("low", "medium", "high", "critical")
_RE_DAILY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_RE_HOURLY = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}$")


@dataclass(frozen=True)
class DatasetDef:
    dataset_id: str
    dataset_name: str
    owner: str
    frequency: str
    expected_offset_minutes: int
    grace_minutes: int
    completion_sla_minutes: int
    min_rows: int
    severity: str

    @property
    def period(self) -> timedelta:
        return timedelta(hours=1) if self.frequency == "hourly" else timedelta(days=1)

    @property
    def period_minutes(self) -> int:
        return int(self.period.total_seconds() // 60)

    # -- partition keys -------------------------------------------------------------------
    def partition_start(self, key: str) -> datetime:
        """Parse a partition key. Raises ValueError for anything not valid for this frequency."""
        if self.frequency == "daily":
            if not _RE_DAILY.match(key):
                raise ValueError(f"daily partition_key must be YYYY-MM-DD, got {key!r}")
            return datetime.strptime(key, "%Y-%m-%d").replace(tzinfo=UTC)
        if not _RE_HOURLY.match(key):
            raise ValueError(f"hourly partition_key must be YYYY-MM-DDTHH, got {key!r}")
        return datetime.strptime(key, "%Y-%m-%dT%H").replace(tzinfo=UTC)

    def key_for(self, start: datetime) -> str:
        return start.strftime("%Y-%m-%d" if self.frequency == "daily" else "%Y-%m-%dT%H")

    # -- schedule -------------------------------------------------------------------------
    def expected_arrival(self, key: str) -> datetime:
        return self.partition_start(key) + timedelta(minutes=self.expected_offset_minutes)

    def arrival_deadline(self, key: str) -> datetime:
        return self.expected_arrival(key) + timedelta(minutes=self.grace_minutes)

    def completion_deadline(self, key: str) -> datetime:
        return self.expected_arrival(key) + timedelta(minutes=self.completion_sla_minutes)

    def max_staleness_minutes(self) -> int:
        """Freshness threshold: one full period + grace + completion SLA (see docs/DESIGN.md)."""
        return self.period_minutes + self.grace_minutes + self.completion_sla_minutes

    def _floor(self, dt: datetime) -> datetime:
        dt = dt.replace(minute=0, second=0, microsecond=0)
        return dt if self.frequency == "hourly" else dt.replace(hour=0)

    def partition_keys(self, as_of: datetime, lookback_hours: int) -> list[str]:
        """Partitions whose expected_arrival lies in (as_of - lookback, as_of]. Sorted ascending."""
        lo = as_of - timedelta(hours=lookback_hours)
        cursor = self._floor(lo) - self.period
        keys: list[str] = []
        while True:
            expected = cursor + timedelta(minutes=self.expected_offset_minutes)
            if expected > as_of:
                break
            if expected > lo:
                keys.append(self.key_for(cursor))
            cursor += self.period
        return keys
