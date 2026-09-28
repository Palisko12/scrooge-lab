"""Basic accounting regression tests for the isolated offline tape audit."""
import tempfile
import unittest
from pathlib import Path
from research.tools.scrooge_lab_trade_audit import summarize


class TapeAuditTests(unittest.TestCase):
    def test_original_window_matches_published_report(self):
        path = Path(__file__).resolve().parents[1] / "forward-test-100" / "trades.csv"
        r = summarize(path, 100)
        self.assertEqual(r["n"], 100)
        self.assertEqual(r["wins"], 90)
        self.assertEqual(r["losses"], 10)
        self.assertEqual(r["reported_realized_usd"], 1793.50)
        self.assertEqual(r["by_source"]["parent"]["n"], 39)
        self.assertEqual(r["by_source"]["popper"]["n"], 58)

    def test_all_rows_includes_extra_outside_window(self):
        path = Path(__file__).resolve().parents[1] / "forward-test-100" / "trades.csv"
        r = summarize(path)
        self.assertEqual(r["n"], 101)
        self.assertEqual(r["reported_realized_usd"], 1770.41)


if __name__ == "__main__":
    unittest.main()
