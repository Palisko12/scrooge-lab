#!/usr/bin/env python3
"""Offline detailed M5 gaps and cross-pair clock analysis.

Shows missing timestamps and examples without presuming weekends are corrupt.
No trading logic, external APIs or historical profitability estimates.
"""
import argparse
import collections
import datetime as dt
import hashlib
import json
import shutil
import tarfile
import tempfile
from pathlib import Path

import pyarrow.parquet as pq

PAIRS = ("USD_JPY", "GBP_USD", "AUD_USD", "EUR_JPY", "USD_CAD", "USD_CHF", "EUR_USD", "AUD_JPY")
STEP = dt.timedelta(minutes=5)
UTC = dt.timezone.utc


def to_utc(raw):
    if isinstance(raw, dt.datetime):
        d = raw
    else:
        d = dt.datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    return d.replace(tzinfo=UTC) if d.tzinfo is None else d.astimezone(UTC)


def extract_clocks(archive):
    clocks = {}
    with tarfile.open(archive, "r:gz") as tar:
        for member in tar:
            name = member.name
            if name.startswith("/") or ".." in Path(name).parts:
                raise ValueError("Unsafe TAR path: " + name)
            if not member.isfile() or Path(name).name.startswith("._"):
                continue
            pair = Path(name).name.removesuffix("_features.parquet")
            if pair not in PAIRS or Path(name).name != pair + "_features.parquet":
                continue
            with tempfile.TemporaryDirectory() as td:
                path = Path(td) / "data.parquet"
                with tar.extractfile(member) as source, path.open("wb") as target:
                    shutil.copyfileobj(source, target)
                pf = pq.ParquetFile(path)
                if "time" not in pf.schema_arrow.names:
                    raise ValueError("No time column: " + name)
                stamps = []
                for batch in pf.iter_batches(batch_size=100_000, columns=["time"]):
                    stamps.extend(to_utc(x) for x in batch.column(0).to_pylist())
                clocks[pair] = stamps
                print(pair, "loaded timestamps", len(stamps), flush=True)
    if set(clocks) != set(PAIRS):
        raise ValueError("Expected all eight QTL pairs, got " + repr(list(clocks)))
    return clocks


def classify_gap(left, right):
    # An explicit market-hour heuristic only, not a market calendar.
    # Friday UTC evening to Sunday UTC evening, with DST-dependent roll.
    if (left.weekday() == 4 and right.weekday() in (6, 0)) or (
        left.weekday() == 5 and right.weekday() in (6, 0)
    ):
        return "weekend_candidate"
    if right - left >= dt.timedelta(hours=12):
        return "extended_other"
    return "intraday"


def audit(clocks):
    clock_sets = {pair: set(stamps) for pair, stamps in clocks.items()}
    universe = set().union(*clock_sets.values())
    all_pairs = set(PAIRS)
    report = {
        "status": "READ_ONLY_GAP_DIAGNOSTICS_NOT_BACKTEST",
        "timezone": "UTC",
        "method": "Observed timeline union of eight instruments, comparison of 5-minute intervals. Weekend classification is heuristic.",
        "pairs": [],
        "common_timestamps": len(set.intersection(*clock_sets.values())),
        "union_timestamps": len(universe),
    }
    for pair in PAIRS:
        stamps = clocks[pair]
        counts = collections.Counter()
        examples = collections.defaultdict(list)
        missing_in_gap = collections.Counter()
        for left, right in zip(stamps, stamps[1:]):
            delta = right - left
            if delta <= STEP:
                continue
            kind = classify_gap(left, right)
            counts[kind] += 1
            absent = int(delta.total_seconds() // 300) - 1
            missing_in_gap[kind] += absent
            if len(examples[kind]) < 8:
                examples[kind].append({
                    "before": left.isoformat(), "after": right.isoformat(),
                    "gap_minutes": int(delta.total_seconds() / 60),
                    "missing_intervals": absent,
                })
        s = clock_sets[pair]
        cross_pair_absent = universe - s
        cross_pair_examples = sorted(cross_pair_absent)[:15]
        report["pairs"].append({
            "pair": pair, "observed_bars": len(stamps),
            "gap_counts": dict(counts),
            "estimated_missing_5m_by_gap_class": dict(missing_in_gap),
            "absent_vs_other_pairs_union": len(cross_pair_absent),
            "absent_vs_union_first15": [x.isoformat() for x in cross_pair_examples],
            "examples": dict(examples),
        })
    report["limitations"] = [
        "The union of pair timestamps is not a ground-truth exchange calendar; missing in all pairs is undetectable.",
        "Weekend and holiday classification is heuristic; no exact holiday/open-hours schedule verified.",
        "No lookahead audit of H1/D precomputed features yet.",
        "No simulation of entries/exits or trading P&L.",
    ]
    return report


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("archive", type=Path)
    ap.add_argument("--output", type=Path, default=Path("scrooge-gap-diagnostics.json"))
    args = ap.parse_args()
    clocks = extract_clocks(args.archive)
    result = audit(clocks)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print("TIMELINE_DIAGNOSTICS")
    print("Common observed timestamps:", result["common_timestamps"])
    print("Union observed timestamps:", result["union_timestamps"])
    for row in result["pairs"]:
        print(json.dumps(row, ensure_ascii=False), flush=True)
