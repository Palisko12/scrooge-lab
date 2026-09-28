#!/usr/bin/env python3
"""Offline audit of upstream published trade tapes. No API keys/network/trading.

This is an ACCOUNTING audit, *not* a historical strategy backtest.
"""
from __future__ import annotations
import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT = ROOT / "forward-test-100" / "trades.csv"


def summarize(path: Path, first_n: int | None = None) -> dict:
    with path.open(newline="", encoding="utf-8-sig") as fp:
        entries = list(csv.DictReader(fp))
    if first_n is not None:
        entries = entries[:first_n]
    if not entries:
        raise ValueError("Empty trade tape")
    rows = []
    for index, row in enumerate(entries, 1):
        try:
            realized = float(row["realized_usd"])
        except (ValueError, KeyError) as e:
            raise ValueError(f"Invalid realized_usd on row {index}") from e
        rows.append((row, realized))
    wins = [pl for _, pl in rows if pl > 0]
    losses = [pl for _, pl in rows if pl < 0]
    by_source = defaultdict(list)
    for row, pl in rows:
        src = row.get("source", "").strip()
        src = src if src in ("parent", "popper") else "legacy_or_unspecified"
        by_source[src].append(pl)
    gross_win = sum(wins)
    gross_loss = -sum(losses)
    # These are reported realized USD, presumed net as recorded by the broker.
    # Without original transactions we cannot independently verify costs.
    return {
        "status": "ACCOUNTING_ONLY_NOT_BACKTEST",
        "n": len(rows),
        "wins": len(wins),
        "losses": len(losses),
        "zero": len(rows) - len(wins) - len(losses),
        "win_rate": round(len(wins) / len(rows), 5),
        "reported_realized_usd": round(sum(pl for _, pl in rows), 2),
        "gross_wins_usd": round(gross_win, 2),
        "gross_losses_usd": round(gross_loss, 2),
        "profit_factor": round(gross_win / gross_loss, 4) if gross_loss else None,
        "mean_win_usd": round(gross_win / len(wins), 2) if wins else None,
        "mean_loss_usd": round(-gross_loss / len(losses), 2) if losses else None,
        "by_source": {
            src: {"n": len(values),
                  "reported_realized_usd": round(sum(values), 2),
                  "wins": sum(p > 0 for p in values),
                  "losses": sum(p < 0 for p in values)}
            for src, values in sorted(by_source.items())
        },
        "limitations": [
            "Published upstream closed-trade tape, not independently verified broker records.",
            "No entry times or entry prices: cannot reconstruct a historical backtest.",
            "No per-trade drawdown or family IDs: cannot infer max portfolio drawdown or group parent/poppers.",
            "No trade-cost breakdown: cannot independently validate net execution costs.",
            "The first 100 closes include a manually closed trade and configuration changes.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=Path, default=DEFAULT)
    parser.add_argument("--first", type=int, default=100,
                        help="Limit to the published 100-trade window; 0=all rows")
    parser.add_argument("--output", type=Path, default=Path("scrooge-trade-audit.json"))
    a = parser.parse_args()
    if a.first < 0:
        parser.error("--first cannot be negative")
    result = summarize(a.csv, None if a.first == 0 else a.first)
    a.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
