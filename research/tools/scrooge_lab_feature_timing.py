#!/usr/bin/env python3
"""Read-only exploratory feature timing audit for QTL M5/H1/D Parquet data.

Checks potential same-bar and higher-timeframe future information fingerprints.
An observed mismatch is diagnostic only; time alignment must be confirmed from
upstream feature generation code. Does not trade or estimate profitability.
"""
import argparse
import collections
import datetime as dt
import json
import math
import shutil
import tarfile
import tempfile
from pathlib import Path

import pyarrow.parquet as pq

PAIRS = ("USD_JPY", "GBP_USD", "AUD_USD", "EUR_JPY",
         "USD_CAD", "USD_CHF", "EUR_USD", "AUD_JPY")
UTC = dt.timezone.utc
M5 = dt.timedelta(minutes=5)
HOUR = dt.timedelta(hours=1)
DAY = dt.timedelta(days=1)
MAX_SAMPLES = 10


def utc(v):
    if isinstance(v, dt.datetime):
        d = v
    else:
        d = dt.datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    return d.replace(tzinfo=UTC) if d.tzinfo is None else d.astimezone(UTC)


def valid_float(x):
    try:
        y = float(x)
        return y if math.isfinite(y) else None
    except (TypeError, ValueError):
        return None


def corr(xy):
    n = len(xy)
    if n < 3:
        return None
    xs, ys = zip(*xy)
    mx, my = sum(xs)/n, sum(ys)/n
    ax = sum((x - mx)**2 for x in xs)
    ay = sum((y - my)**2 for y in ys)
    if ax <= 0 or ay <= 0:
        return None
    return sum((x-mx)*(y-my) for x, y in xy) / math.sqrt(ax*ay)


def inspect(path, name):
    pf = pq.ParquetFile(path)
    columns = pf.schema_arrow.names
    desired = [c for c in columns if
               c in ("time","open","high","low","close","d_high","d_low","d_close")
               or c.lower().startswith(("h1_", "d_", "trend_1h", "trend_4h", "atr_1h", "atr_4h"))
               or any(q in c.lower() for q in ("lookahead","future","leak","hour","daily"))]
    if "time" not in desired:
        return {"file":name,"error":"Missing time"}
    wanted = list(dict.fromkeys(desired))
    observations = {c: [] for c in wanted}
    for batch in pf.iter_batches(columns=wanted, batch_size=100_000):
        data = batch.to_pydict()
        for c in wanted:
            observations[c].extend(data[c])
    stamps = [utc(t) for t in observations["time"]]
    result = {
        "file":name, "rows":len(stamps), "candidate_columns":wanted,
        "time_convention":"UNKNOWN (bar open or close not independently verified)",
        "sample_feature_rows":[], "daily_same_period_matches":{},
        "limitations":[
            "Descriptive timing fingerprint only. No independent source-candle parity.",
            "A same-period daily OHLC value may be expected or leaked depending on its documented signal availability.",
            "No verification of feature code, H1 join offsets, or point-in-time execution semantics.",
        ],
    }
    for i in [0, 1, 2, 10, 100, len(stamps)//2, len(stamps)-1]:
        if not 0 <= i < len(stamps):
            continue
        row = {"time":stamps[i].isoformat()}
        for k in wanted:
            if k != "time":
                v = observations[k][i]
                row[k] = str(v)[:160] if not isinstance(v, (int,float,type(None),bool)) else v
        result["sample_feature_rows"].append(row)
    for field in ("d_high","d_low","d_close"):
        if field not in observations:
            continue
        same_period = 0
        future_change_within_day = 0
        last_day = None
        prev_val = None
        first = None
        last = None
        count = 0
        samples = []
        for t, raw in zip(stamps, observations[field]):
            v = valid_float(raw)
            if v is None:
                continue
            day = t.date()
            if day != last_day:
                if last_day is not None and count > 1 and first != last:
                    future_change_within_day += 1
                last_day, first, count = day, v, 0
            last = v
            count += 1
        if last_day is not None and count > 1 and first != last:
            future_change_within_day += 1
        result["daily_same_period_matches"][field] = {
            "utc_calendar_days_value_changes_within_day":future_change_within_day,
            "note":"A UTC calendar day is not necessarily the broker's D candle; interpret only after verifying feature generator."
        }
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("archive",type=Path)
    ap.add_argument("--output",type=Path,default=Path("scrooge-feature-timing.json"))
    args = ap.parse_args()
    report = {
      "audit":"FEATURE_TIMING_EXPLORATION_ONLY",
      "pair_reports":[],
      "explicit_no_claims":"No point-in-time safety certification, no live trades, no P&L",
    }
    with tarfile.open(args.archive,"r:gz") as tar:
        for m in tar:
            if m.name.startswith("/") or ".." in Path(m.name).parts:
                raise ValueError("Unsafe tar member: "+m.name)
            name = Path(m.name).name
            pair = name.removesuffix("_features.parquet")
            if not m.isfile() or pair not in PAIRS or name!=pair+"_features.parquet":
                continue
            with tempfile.TemporaryDirectory() as d:
                dest = Path(d)/"one.parquet"
                with tar.extractfile(m) as src,dest.open("wb") as out:
                    shutil.copyfileobj(src,out)
                item = inspect(dest,m.name)
                report["pair_reports"].append(item)
                print("FEATURE_TIMING",item["file"],item["rows"],
                    "columns=",item["candidate_columns"],
                    "daily_diagnostics=",item["daily_same_period_matches"],flush=True)
    args.output.write_text(json.dumps(report,indent=2,ensure_ascii=False,default=str)+"\n")
    if len(report["pair_reports"])!=len(PAIRS):
        raise RuntimeError(f"Expected {len(PAIRS)} pairs, got {len(report['pair_reports'])}")


if __name__=="__main__":
    main()
