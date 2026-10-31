"""Strict ISO-8601 helpers. All internal timestamps are UTC, rendered as YYYY-MM-DDTHH:MM:SSZ.

Because the rendered format is fixed-width and UTC, string comparison == chronological comparison,
which the SQL layer relies on (``event_ts <= :as_of``).
"""
from __future__ import annotations

from datetime import datetime, timezone

ISO_Z = "%Y-%m-%dT%H:%M:%SZ"
UTC = timezone.utc


def parse_ts(value: str) -> datetime:
    """Parse an ISO-8601 timestamp that MUST carry a timezone. Returns an aware UTC datetime.

    Naive timestamps are rejected on purpose: silently assuming a zone is how SLA breaches get
    hidden or invented. Sub-second precision is truncated to whole seconds.
    """
    if value is None:
        raise ValueError("timestamp is null")
    v = value.strip()
    if not v:
        raise ValueError("timestamp is empty")
    if v.endswith(("Z", "z")):
        v = v[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(v)
    except ValueError as exc:
        raise ValueError(f"not an ISO-8601 timestamp: {value!r}") from exc
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"timestamp has no timezone offset: {value!r}")
    return dt.astimezone(UTC).replace(microsecond=0)


def fmt_ts(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime(ISO_Z)


def from_iso_z(s: str) -> datetime:
    """Fast parser for timestamps *we* wrote (fixed format)."""
    return datetime.fromisoformat(s[:-1]).replace(tzinfo=UTC)


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)
