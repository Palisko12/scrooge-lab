#!/usr/bin/env python3
"""Offline structural audit of the published allbars Parquet archive.

Does not import the bot, connect to a broker, or submit orders.
"""
import argparse
import json
import tarfile
import tempfile
from pathlib import Path

import pyarrow.parquet as pq


def audit(archive: Path) -> dict:
    if not tarfile.is_tarfile(archive):
        raise ValueError("Downloaded file is not a valid tar archive")
    results = []
    with tarfile.open(archive, "r:gz") as tf:
        members = [m for m in tf.getmembers() if m.isfile()]
        for member in members:
            if member.name.startswith("/") or ".." in Path(member.name).parts:
                raise ValueError("Unsafe archive member: " + member.name)
            if not member.name.endswith(".parquet"):
                continue
            with tempfile.TemporaryDirectory() as td:
                target = Path(td) / "input.parquet"
                source = tf.extractfile(member)
                if source is None:
                    raise ValueError("Missing member: " + member.name)
                with source, target.open("wb") as out:
                    import shutil
                    shutil.copyfileobj(source, out)
                pf = pq.ParquetFile(target)
                names = pf.schema_arrow.names
                low = [n.lower() for n in names]
                time_names = [n for n in names if any(k in n.lower() for k in ("time", "date", "timestamp"))]
                quote_names = [n for n in names if "bid" in n.lower() or "ask" in n.lower()]
                row = {
                    "file": member.name,
                    "rows": pf.metadata.num_rows,
                    "row_groups": pf.metadata.num_row_groups,
                    "columns": names,
                    "timestamp_candidates": time_names,
                    "bid_ask_candidates": quote_names,
                    "schema": str(pf.schema_arrow),
                }
                if time_names:
                    tcol = time_names[0]
                    min_t, max_t, nulls, dup, backwards = None, None, 0, 0, 0
                    previous = None
                    for batch in pf.iter_batches(batch_size=100000, columns=[tcol]):
                        for val in batch.column(0).to_pylist():
                            if val is None:
                                nulls += 1
                                continue
                            s = str(val)
                            if min_t is None or s < min_t:
                                min_t = s
                            if max_t is None or s > max_t:
                                max_t = s
                            if previous is not None:
                                if s == previous:
                                    dup += 1
                                elif s < previous:
                                    backwards += 1
                            previous = s
                    row.update(first_timestamp=min_t, last_timestamp=max_t,
                               null_timestamps=nulls, consecutive_duplicate_timestamps=dup,
                               out_of_order_timestamps=backwards)
                results.append(row)
    if not results:
        raise ValueError("No parquet files in archive")
    return {
        "status": "DATA_STRUCTURE_AUDIT_NOT_BACKTEST",
        "parquet_files": len(results),
        "total_rows": sum(r["rows"] for r in results),
        "files": results,
        "limitations": [
            "No strategy entries/exits or P&L are simulated.",
            "Only consecutive duplicate timestamps are checked; non-adjacent duplicates need follow-up.",
            "Timestamp ordering is compared as string representations; timezone normalization needs follow-up.",
            "No independent verification of quote provenance or realistic execution costs.",
        ],
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("archive", type=Path)
    p.add_argument("--output", type=Path, default=Path("scrooge-data-audit.json"))
    args = p.parse_args()
    result = audit(args.archive)
    args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
