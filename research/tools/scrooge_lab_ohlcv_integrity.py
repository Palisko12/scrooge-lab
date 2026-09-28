#!/usr/bin/env python3
"""Offline read-only OHLC integrity check of QTL pair feature tables.

This intentionally DOES NOT certify precomputed feature point-in-time safety,
historical bid/ask provenance, or strategy profitability.
"""
import argparse
import hashlib
import json
import math
import shutil
import tarfile
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

PAIR_NAMES = {"USD_JPY", "GBP_USD", "AUD_USD", "EUR_JPY",
              "USD_CAD", "USD_CHF", "EUR_USD", "AUD_JPY"}
OHLC = ("open", "high", "low", "close")
TIME_CANDIDATES = ("time", "timestamp", "datetime", "date", "bar_time", "candle_time")


def timestamp_to_us(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return int(v.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
        return int(v.timestamp() * 1_000_000)
    if isinstance(v, (int, float)):
        val = float(v)
        if not math.isfinite(val):
            return None
        scale = 1_000_000 if abs(val) < 1e11 else (1000 if abs(val) < 1e14 else 1)
        return int(val * scale)
    s = str(v).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(s)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1_000_000)
    except ValueError:
        return None


def check_pair(p, name):
    pf = pq.ParquetFile(p)
    columns = pf.schema_arrow.names
    missing = [c for c in OHLC if c not in columns]
    if missing:
        return {"file": name, "status": "MISSING_OHLC", "columns": columns, "missing": missing}
    tcol = next((c for c in TIME_CANDIDATES if c in columns), None)
    if tcol is None:
        return {"file": name, "status": "UNKNOWN_TIMESTAMP", "columns": columns,
                "candidates": [c for c in columns if "time" in c.lower() or "date" in c.lower()]}
    selected = list(OHLC) + [tcol]
    volume_name = "volume" if "volume" in columns else None
    if volume_name:
        selected.append(volume_name)
    result = {
        "file": name, "status": "INSPECTED", "rows": pf.metadata.num_rows,
        "time_col": tcol, "volume_col": volume_name, "min_utc": None, "max_utc": None,
        "null_or_invalid_time": 0, "duplicate_timestamp": 0, "out_of_order": 0,
        "nonpositive_ohlc": 0, "null_or_nonfinite_ohlc": 0,
        "high_below_open_close_low": 0, "low_above_open_close_high": 0,
        "negative_volume": 0, "nonfinite_volume": 0,
        "gaps_over_5m": 0, "gaps_over_1h": 0, "largest_gap_minutes": 0,
        "delta_minutes_top": [], "examples": {},
    }
    previous = None
    minimum = maximum = None
    delta_counts = Counter()
    last_examples = Counter()
    for batch in pf.iter_batches(columns=selected, batch_size=50000):
        data = batch.to_pydict()
        for idx in range(batch.num_rows):
            ts = timestamp_to_us(data[tcol][idx])
            if ts is None:
                result["null_or_invalid_time"] += 1
                continue
            minimum = ts if minimum is None else min(minimum, ts)
            maximum = ts if maximum is None else max(maximum, ts)
            if previous is not None:
                delta = ts - previous
                if delta == 0:
                    result["duplicate_timestamp"] += 1
                elif delta < 0:
                    result["out_of_order"] += 1
                else:
                    mins = delta / 60_000_000
                    if mins > 5:
                        result["gaps_over_5m"] += 1
                    if mins > 60:
                        result["gaps_over_1h"] += 1
                    result["largest_gap_minutes"] = max(result["largest_gap_minutes"], mins)
                    delta_counts[round(mins, 3)] += 1
            previous = ts
            prices = [data[k][idx] for k in OHLC]
            try:
                nums = [float(x) for x in prices]
            except (TypeError, ValueError):
                result["null_or_nonfinite_ohlc"] += 1
                continue
            if not all(math.isfinite(x) for x in nums):
                result["null_or_nonfinite_ohlc"] += 1
                continue
            o, h, l, c = nums
            if min(nums) <= 0:
                result["nonpositive_ohlc"] += 1
            if h < max(o, c, l) - 1e-10:
                result["high_below_open_close_low"] += 1
            if l > min(o, c, h) + 1e-10:
                result["low_above_open_close_high"] += 1
            if volume_name:
                v = data[volume_name][idx]
                try:
                    vv = float(v)
                    if not math.isfinite(vv):
                        result["nonfinite_volume"] += 1
                    elif vv < 0:
                        result["negative_volume"] += 1
                except (TypeError, ValueError):
                    result["nonfinite_volume"] += 1
    if minimum is not None:
        result["min_utc"] = datetime.fromtimestamp(minimum / 1_000_000, timezone.utc).isoformat()
        result["max_utc"] = datetime.fromtimestamp(maximum / 1_000_000, timezone.utc).isoformat()
    result["delta_minutes_top"] = [{"minutes": d, "count": n} for d, n in delta_counts.most_common(10)]
    if result["out_of_order"] or result["duplicate_timestamp"] or result["null_or_invalid_time"]:
        result["status"] = "TIMESTAMP_ISSUES_REVIEW"
    if result["null_or_nonfinite_ohlc"] or result["high_below_open_close_low"] or result["low_above_open_close_high"]:
        result["status"] = "PRICE_ISSUES_REVIEW"
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument("--output", type=Path, default=Path("scrooge-ohlcv-integrity.json"))
    args = parser.parse_args()
    report = {"audit": "SOURCE_OHLCV_ONLY_NOT_BACKTEST",
              "pairs": [], "limitations": [
                  "No independent quote provenance or bid/ask spreads available.",
                  "Precomputed H1/D features still require point-in-time join audit.",
                  "Weekend/holiday gaps are counted; they are not automatically missing data.",
                  "No trading simulation, predictive labels, or P&L.",
              ]}
    digest = hashlib.sha256()
    with args.archive.open("rb") as f:
        for block in iter(lambda: f.read(4 * 1024 * 1024), b""):
            digest.update(block)
    report["archive_sha256"] = digest.hexdigest()
    with tarfile.open(args.archive, "r:gz") as tar:
        for m in tar:
            if m.name.startswith("/") or ".." in Path(m.name).parts:
                raise ValueError("Unsafe path: " + m.name)
            if not m.isfile() or m.name.split("/")[-1].startswith("._"):
                continue
            filename = Path(m.name).name
            pair = filename.removesuffix("_features.parquet")
            if pair not in PAIR_NAMES or filename != pair + "_features.parquet":
                continue
            with tempfile.TemporaryDirectory() as td:
                path = Path(td) / filename
                with tar.extractfile(m) as src, path.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
                finding = check_pair(path, m.name)
                print(json.dumps(finding, ensure_ascii=False), flush=True)
                report["pairs"].append(finding)
    report["count"] = len(report["pairs"])
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    if report["count"] != 8:
        raise RuntimeError(f"Expected 8 pairs; got {report['count']}")


if __name__ == "__main__":
    main()
