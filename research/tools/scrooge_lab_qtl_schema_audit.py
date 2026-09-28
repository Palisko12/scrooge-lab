#!/usr/bin/env python3
"""Read-only manifest and schema inspection of the upstream QTL research corpus.

No bot imports, credentials, or broker access. No profitability claims.
"""
import argparse
import hashlib
import json
import shutil
import tarfile
import tempfile
from pathlib import Path

import pyarrow.parquet as pq


def inspect(archive: Path):
    digest = hashlib.sha256()
    with archive.open("rb") as f:
        for chunk in iter(lambda: f.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    out = {"status": "SCHEMA_AUDIT_ONLY_NOT_BACKTEST",
           "archive_sha256": digest.hexdigest(), "archive_bytes": archive.stat().st_size,
           "members": [], "parquet_files": [], "parquet_errors": []}
    with tarfile.open(archive, "r:gz") as tar:
        for m in tar:
            if m.name.startswith("/") or ".." in Path(m.name).parts:
                raise ValueError("Unsafe member path: " + m.name)
            if not m.isfile():
                continue
            out["members"].append({"name": m.name, "bytes": m.size})
            if not m.name.lower().endswith(".parquet"):
                continue
            with tempfile.TemporaryDirectory() as td:
                p = Path(td) / "one.parquet"
                with tar.extractfile(m) as src, p.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
                try:
                    pf = pq.ParquetFile(p)
                except Exception as exc:
                    with p.open("rb") as raw:
                        head = raw.read(16).hex()
                        raw.seek(max(0, p.stat().st_size - 16))
                        foot = raw.read(16).hex()
                    problem = {"name": m.name, "bytes": m.size, "error_type": type(exc).__name__,
                               "error": str(exc), "head16_hex": head, "tail16_hex": foot}
                    print("PARQUET_READ_ERROR", json.dumps(problem), flush=True)
                    out["parquet_errors"].append(problem)
                    continue
                names = pf.schema_arrow.names
                out["parquet_files"].append({
                    "name": m.name,
                    "rows": pf.metadata.num_rows,
                    "row_groups": pf.metadata.num_row_groups,
                    "columns": names,
                    "timestamp_candidates": [c for c in names if any(x in c.lower() for x in ("time", "date", "timestamp"))],
                    "ohlcv_candidates": [c for c in names if any(x in c.lower() for x in ("open", "high", "low", "close", "volume"))],
                    "bid_ask_candidates": [c for c in names if "bid" in c.lower() or "ask" in c.lower()],
                })
    out["member_count"] = len(out["members"])
    out["parquet_count"] = len(out["parquet_files"])
    out["parquet_error_count"] = len(out["parquet_errors"])
    out["total_parquet_rows"] = sum(p["rows"] for p in out["parquet_files"])
    out["limitations"] = [
        "Schema and metadata only; timestamps, OHLC consistency, missing bars, and leakage NOT YET validated.",
        "Source labels of CLEAN are not independently verified.",
        "No entries/exits, transaction costs, or P&L simulated.",
    ]
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("archive", type=Path)
    ap.add_argument("--output", type=Path, default=Path("scrooge-qtl-schema-audit.json"))
    args = ap.parse_args()
    result = inspect(args.archive)
    args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print("Archive SHA256:", result["archive_sha256"])
    print("Archive members:", result["member_count"])
    print("Parquet files:", result["parquet_count"])
    print("Unreadable Parquet files:", result["parquet_error_count"])
    print("Total parquet rows:", result["total_parquet_rows"])
    for p in result["parquet_files"]:
        print(p["name"], "rows=", p["rows"], "OHLCV=", p["ohlcv_candidates"], "bid/ask=", p["bid_ask_candidates"])
