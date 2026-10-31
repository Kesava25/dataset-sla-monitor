"""Deterministic synthetic data generator (fixed seed => byte-identical output).

Writes, under <out>:
  dataset_registry.csv         SLA definitions (+ deliberately invalid rows)
  landing/load_events_*.csv    load-event logs, one file per UTC day, shuffled (out-of-order)
  faults/*.csv                 whole-file faults used by the demo/tests (NOT in landing/)
  expected/                    ANSWER KEY, kept apart from the input:
      expected_labels.csv        per-partition expected statuses (from the scenario that produced it)
      expected_rejects.csv       every injected bad record -> (file, line, reason_code)
      expected_summary.json      expected row counts per file and in total
"""
from __future__ import annotations

import csv
import json
import random
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .schedule import DatasetDef
from .timeutil import fmt_ts, parse_ts

NAMES = ["orders", "customers", "payments", "inventory", "shipments", "web_events",
         "returns", "products", "suppliers", "invoices", "campaigns", "sessions"]
EVENT_HEADER = ["dataset_id", "partition_key", "event_type", "event_ts", "row_count"]
REG_HEADER = ["dataset_id", "dataset_name", "owner", "frequency", "expected_offset_minutes",
              "grace_minutes", "completion_sla_minutes", "min_rows", "severity"]
OWNERS = ["data-platform", "growth-analytics", "finance-data", "supply-chain"]
IST = timezone(timedelta(hours=5, minutes=30))

# scenario -> (arrival_status, completion_status, sla_status, low_row_count, arrival_inferred)
LABELS = {
    "ON_TIME": ("ON_TIME", "ON_TIME", "MET", 0, 0),
    "LATE_ARRIVAL": ("LATE", "ON_TIME", "BREACHED", 0, 0),
    "LATE_COMPLETION": ("ON_TIME", "LATE", "BREACHED", 0, 0),
    "MISSING": ("MISSING", "OVERDUE", "BREACHED", 0, 0),
    "FAILED": ("ON_TIME", "FAILED", "BREACHED", 0, 0),
    "OVERDUE": ("ON_TIME", "OVERDUE", "BREACHED", 0, 0),
    "LOW_ROW_COUNT": ("ON_TIME", "ON_TIME", "MET", 1, 0),
    "RETRY_ON_TIME": ("ON_TIME", "ON_TIME", "MET", 0, 0),
    "NO_ARRIVAL_EVENT": ("ON_TIME", "ON_TIME", "MET", 0, 1),
    "FUTURE_ARRIVAL": ("MISSING", "OVERDUE", "BREACHED", 0, 0),
    "PENDING_ARRIVED": ("ON_TIME", "PENDING", "PENDING", 0, 0),
    "PENDING_MISSING": ("MISSING", "PENDING", "BREACHED", 0, 0),
    "ON_TIME_EARLY": ("ON_TIME", "ON_TIME", "MET", 0, 0),
    "GRACE_NONE": ("PENDING", "PENDING", "PENDING", 0, 0),
    "GRACE_ARRIVED": ("ON_TIME", "PENDING", "PENDING", 0, 0),
    "GRACE_DONE": ("ON_TIME", "ON_TIME", "MET", 0, 0),
}
MATURE_W = [("ON_TIME", .86), ("LATE_ARRIVAL", .03), ("LATE_COMPLETION", .03), ("MISSING", .015), ("FAILED", .015),
            ("OVERDUE", .01), ("LOW_ROW_COUNT", .01), ("RETRY_ON_TIME", .015), ("NO_ARRIVAL_EVENT", .01),
            ("FUTURE_ARRIVAL", .005)]
ARR_MATURE_W = [("PENDING_ARRIVED", .4), ("PENDING_MISSING", .2), ("ON_TIME_EARLY", .4)]
GRACE_W = [("GRACE_NONE", .4), ("GRACE_ARRIVED", .3), ("GRACE_DONE", .3)]


@dataclass
class GenParams:
    seed: int = 42
    datasets: int = 8
    days: int = 7
    as_of: str = "2026-01-15T07:40:00Z"
    hourly_fraction: float = 0.5
    anomalies: bool = True


def _pick(rng: random.Random, weights):
    r, acc = rng.random(), 0.0
    for name, w in weights:
        acc += w
        if r < acc:
            return name
    return weights[-1][0]


def _make_datasets(rng: random.Random, p: GenParams) -> list[DatasetDef]:
    out = []
    n_hourly = round(p.datasets * p.hourly_fraction)
    for i in range(p.datasets):
        base = NAMES[i % len(NAMES)]
        did = f"ds_{base}" if i < len(NAMES) else f"ds_{base}_{i // len(NAMES) + 1:02d}"
        hourly = i < n_hourly
        grace = rng.randint(10, 20) if hourly else rng.randint(30, 60)
        out.append(DatasetDef(
            dataset_id=did, dataset_name=f"{base.replace('_', ' ').title()} feed", owner=OWNERS[i % len(OWNERS)],
            frequency="hourly" if hourly else "daily",
            expected_offset_minutes=rng.randint(5, 30) if hourly else rng.randint(120, 480),
            grace_minutes=grace, completion_sla_minutes=grace + (rng.randint(15, 45) if hourly else rng.randint(60, 180)),
            min_rows=rng.randint(50, 300) if hourly else rng.randint(500, 5000),
            severity=rng.choices(["critical", "high", "medium", "low"], [.2, .3, .3, .2])[0]))
    return out


def _partition_events(rng, d: DatasetDef, key: str, as_of: datetime, force: str | None = None):
    """Return (scenario, [(event_type, ts, row_count)]) for one partition. `force` pins a mature scenario."""
    E, A, C = d.expected_arrival(key), d.arrival_deadline(key), d.completion_deadline(key)
    sec = lambda x, y: x + timedelta(seconds=rng.randint(0, max(0, int((y - x).total_seconds()))))  # noqa: E731
    rows = lambda: rng.randint(d.min_rows, d.min_rows * 5)  # noqa: E731
    one_min = timedelta(minutes=1)

    def on_time_pair():
        arr = A if rng.random() < .05 else sec(E, A)                  # 5%: exactly on the boundary
        comp = C if rng.random() < .05 else sec(arr + one_min, C)
        return arr, comp

    if as_of > C:
        sc = force or _pick(rng, MATURE_W)
        if sc == "LATE_COMPLETION" and (as_of - C).total_seconds() < 120:
            sc = "ON_TIME"
        if sc == "ON_TIME" or sc == "LOW_ROW_COUNT":
            arr, comp = on_time_pair()
            rc = rows() if sc == "ON_TIME" else rng.randint(0, d.min_rows - 1)
            return sc, [("ARRIVED", arr, None), ("COMPLETED", comp, rc)]
        if sc == "LATE_ARRIVAL":
            arr = A + timedelta(seconds=rng.randint(60, int((C - A).total_seconds()) - 120))
            return sc, [("ARRIVED", arr, None), ("COMPLETED", sec(arr + one_min, C), rows())]
        if sc == "LATE_COMPLETION":
            arr = sec(E, A)
            return sc, [("ARRIVED", arr, None),
                        ("COMPLETED", C + timedelta(seconds=rng.randint(1, min(10800, int((as_of - C).total_seconds())))), rows())]
        if sc == "MISSING":
            return sc, []
        if sc == "FAILED":
            arr = sec(E, A)
            return sc, [("ARRIVED", arr, None), ("FAILED", arr + timedelta(seconds=rng.randint(60, 300)), None)]
        if sc == "OVERDUE":
            return sc, [("ARRIVED", sec(E, A), None)]
        if sc == "RETRY_ON_TIME":
            arr = sec(E, A)
            return sc, [("ARRIVED", arr, None), ("FAILED", arr + timedelta(seconds=rng.randint(60, 240)), None),
                        ("COMPLETED", sec(arr + timedelta(minutes=5), C), rows())]
        if sc == "NO_ARRIVAL_EVENT":
            return sc, [("COMPLETED", sec(E, A), rows())]
        if sc == "FUTURE_ARRIVAL":
            arr = as_of + timedelta(seconds=rng.randint(60, 18000))
            return sc, [("ARRIVED", arr, None), ("COMPLETED", arr + timedelta(minutes=5), rows())]
    if as_of > A:
        sc = _pick(rng, ARR_MATURE_W)
        arr = sec(E, A)
        if sc == "ON_TIME_EARLY" and (as_of - arr).total_seconds() < 60:
            sc = "PENDING_ARRIVED"
        if sc == "PENDING_ARRIVED":
            return sc, [("ARRIVED", arr, None)]
        if sc == "PENDING_MISSING":
            return sc, []
        return sc, [("ARRIVED", arr, None), ("COMPLETED", sec(arr + one_min, as_of), rows())]
    sc = _pick(rng, GRACE_W)
    arr = sec(E, as_of)
    if sc == "GRACE_DONE" and (as_of - arr).total_seconds() < 60:
        sc = "GRACE_ARRIVED"
    if sc == "GRACE_NONE":
        return sc, []
    if sc == "GRACE_ARRIVED":
        return sc, [("ARRIVED", arr, None)]
    return sc, [("ARRIVED", arr, None), ("COMPLETED", sec(arr + one_min, as_of), rows())]


def _idx(rows: list, obj) -> int:
    return next(i for i, r in enumerate(rows) if r is obj)


def _ts_text(rng, ts: datetime) -> str:
    """~10% of rows use a non-UTC offset for the *same instant* (exercises timezone normalisation)."""
    return ts.astimezone(IST).isoformat() if rng.random() < .10 else fmt_ts(ts)


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def generate(out_dir: str | Path, p: GenParams | None = None) -> dict:
    p = p or GenParams()
    out = Path(out_dir)
    rng = random.Random(p.seed)
    as_of = parse_ts(p.as_of)
    datasets = _make_datasets(rng, p)

    # ---- registry (+ invalid rows, labelled in expected_rejects) ---------------------------------
    reg_rows = [[d.dataset_id, d.dataset_name, d.owner, d.frequency, d.expected_offset_minutes, d.grace_minutes,
                 d.completion_sla_minutes, d.min_rows, d.severity] for d in datasets]
    expected_rejects: list[tuple] = []
    if p.anomalies:
        bad = [
            (["ds_bad_frequency", "Bad", "x", "weekly", 10, 10, 30, 1, "low"], "INVALID_FREQUENCY"),
            (["ds_bad_sla", "Bad", "x", "daily", 100, 60, 30, 1, "low"], "INVALID_SLA_DEFINITION"),
            ([datasets[0].dataset_id, "Duplicate id", "x", "daily", 100, 10, 30, 1, "low"], "DUPLICATE_DATASET_ID"),
        ]
        for row, code in bad:
            reg_rows.append(row)
            expected_rejects.append(("registry", "dataset_registry.csv", len(reg_rows) + 1, code))
    _write_csv(out / "dataset_registry.csv", REG_HEADER, reg_rows)

    # ---- partitions -> events + labels ------------------------------------------------------------
    lookback = p.days * 24
    labels: list[list] = []
    by_day: dict[str, list[dict]] = {}
    parts = [(d, key) for d in datasets for key in d.partition_keys(as_of, lookback)]
    # Coverage guarantee: every non-trivial mature scenario appears at least once (pinned to partitions
    # that finished >= 4h before as_of so no scenario has to fall back), whatever the seed or size.
    roomy = [i for i, (d, k) in enumerate(parts) if as_of - d.completion_deadline(k) >= timedelta(hours=4)]
    forced: dict[int, str] = {}
    for name, _ in MATURE_W[1:]:
        free = [i for i in roomy if i not in forced]
        if free:
            forced[rng.choice(free)] = name
    for i, (d, key) in enumerate(parts):
        sc, evs = _partition_events(rng, d, key, as_of, forced.get(i))
        a, c, s, low, inf = LABELS[sc]
        labels.append([d.dataset_id, key, sc, a, c, s, low, inf])
        for et, ts, rc in evs:
            by_day.setdefault(ts.strftime("%Y%m%d"), []).append({
                "f": [d.dataset_id, key, et, _ts_text(rng, ts), "" if rc is None else str(rc)],
                "tag": None, "type": et, "ds": d})

    # ---- per-file shuffle + anomaly injection ---------------------------------------------------------
    days = sorted(by_day)
    clean_by_day: dict[str, list[dict]] = {}
    files: dict[str, list[dict]] = {}
    for day in days:
        rows = by_day[day]
        rng.shuffle(rows)                               # out-of-order delivery
        clean_by_day[day] = list(rows)
        files[day] = rows
    for i, day in enumerate(days):
        if not p.anomalies:
            continue
        rows = files[day]
        base = list(clean_by_day[day])
        k = max(1, int(len(base) * 0.003))

        def insert_anywhere(row):
            rows.insert(rng.randint(0, len(rows)), row)

        def clone(src, tag, over=None):
            f = list(src["f"])
            for idx, val in (over or {}).items():
                f[idx] = val
            return {"f": f, "tag": tag, "type": src["type"], "ds": src["ds"]}

        if i > 0:                                        # cross-file replay -> already loaded, NOT rejected
            for src in rng.sample(clean_by_day[days[i - 1]], min(k, len(clean_by_day[days[i - 1]]))):
                insert_anywhere(clone(src, "ALREADY_LOADED"))
        for src in rng.sample(base, min(k, len(base))):  # exact duplicate AFTER its original
            rows.insert(rng.randint(_idx(rows, src) + 1, len(rows)), clone(src, "DUPLICATE_EVENT"))
        comp = [r for r in base if r["type"] == "COMPLETED"]
        for src in rng.sample(comp, min(k, len(comp))):  # conflicting duplicate (different row_count)
            rows.insert(rng.randint(_idx(rows, src) + 1, len(rows)),
                        clone(src, "DUPLICATE_EVENT", {4: str(int(src["f"][4]) + 7)}))
        for src in rng.sample(base, min(k, len(base))):  # blank required field
            insert_anywhere(clone(src, "MISSING_REQUIRED_FIELD", {rng.choice([0, 1, 2, 3]): ""}))
        for src in rng.sample(comp, min(k, len(comp))):  # COMPLETED without row_count
            insert_anywhere(clone(src, "MISSING_REQUIRED_FIELD", {4: ""}))
        for src in rng.sample(base, min(k, len(base))):
            bad_ts = rng.choice(["2026-13-45T99:00:00Z", "not-a-date", src["f"][3].replace("Z", "").split("+")[0]])
            insert_anywhere(clone(src, "INVALID_TIMESTAMP", {3: bad_ts}))
        for src in rng.sample(base, min(k, len(base))):
            insert_anywhere(clone(src, "INVALID_EVENT_TYPE", {2: rng.choice(["ARIVED", "STARTED"])}))
        for src in rng.sample(base, min(k, len(base))):
            insert_anywhere(clone(src, "UNKNOWN_DATASET", {0: "ds_unknown_feed"}))
        for src in rng.sample(base, min(k, len(base))):
            wrong = "2026-01-10T05" if src["ds"].frequency == "daily" else "2026-01-10"
            insert_anywhere(clone(src, "INVALID_PARTITION_KEY", {1: wrong}))
        for src in rng.sample(comp, min(k, len(comp))):
            insert_anywhere(clone(src, "INVALID_ROW_COUNT", {4: rng.choice(["-5", "abc", "12.5"])}))
        for src in rng.sample(base, min(k, len(base))):  # wrong number of fields
            f = src["f"][:3] if rng.random() < .5 else src["f"] + ["unexpected", "extra"]
            insert_anywhere({"f": f, "tag": "MALFORMED_ROW", "type": src["type"], "ds": src["ds"]})

    # ---- write landing files + expected ---------------------------------------------------------------
    per_file = {}
    for day in days:
        name = f"load_events_{day}.csv"
        rows = files[day]
        _write_csv(out / "landing" / name, EVENT_HEADER, [r["f"] for r in rows])
        cnt = Counter("rejected" if r["tag"] not in (None, "ALREADY_LOADED") else
                      ("already_loaded" if r["tag"] else "loaded") for r in rows)
        per_file[name] = {"rows_read": len(rows), "rows_loaded": cnt["loaded"],
                          "rows_already_loaded": cnt["already_loaded"], "rows_rejected": cnt["rejected"]}
        for n, r in enumerate(rows):
            if r["tag"] not in (None, "ALREADY_LOADED"):
                expected_rejects.append(("event", name, n + 2, r["tag"]))

    sample_rows = [r["f"] for r in files[days[0]] if r["tag"] is None][:2] if days else []
    _write_faults(out / "faults", sample_rows)
    exp = out / "expected"
    _write_csv(exp / "expected_labels.csv",
               ["dataset_id", "partition_key", "scenario", "expected_arrival_status", "expected_completion_status",
                "expected_sla_status", "expected_low_row_count", "expected_arrival_inferred"], labels)
    _write_csv(exp / "expected_rejects.csv", ["entity", "source_file", "source_line", "reason_code"],
               [list(x) for x in sorted(expected_rejects)])
    summary = {
        "params": p.__dict__,
        "partitions": len(labels),
        "scenario_counts": dict(sorted(Counter(r[2] for r in labels).items())),
        "totals": {k: sum(v[k] for v in per_file.values()) for k in
                   ("rows_read", "rows_loaded", "rows_already_loaded", "rows_rejected")},
        "reject_reason_counts": dict(sorted(Counter(x[3] for x in expected_rejects).items())),
        "per_file": per_file,
    }
    (exp / "expected_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def _write_faults(d: Path, sample_rows: list[list[str]]) -> None:
    d.mkdir(parents=True, exist_ok=True)
    _write_csv(d / "bad_schema.csv", ["dataset_id", "partition_key", "event_type", "event_time", "row_count"],
               [["ds_orders", "2026-01-14T02", "ARRIVED", "2026-01-14T02:12:00Z", ""]])
    (d / "empty_file.csv").write_bytes(b"")
    _write_csv(d / "header_only.csv", EVENT_HEADER, [])
    (d / "not_utf8.csv").write_bytes(b"\xff\xfe\x00garbage\x80\x81")
    _write_csv(d / "extra_column.csv", EVENT_HEADER + ["producer_version"], [r + ["v2"] for r in sample_rows])
