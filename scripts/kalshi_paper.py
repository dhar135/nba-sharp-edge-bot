"""Paper-trade Kalshi: log (our calibrated prob) vs (market price) for
overlapping player-prop markets. No orders are ever placed.
Usage: python scripts/kalshi_paper.py <series_ticker>"""
import os
import sqlite3
import sys
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from venues.kalshi import get_markets, normalize_market, taker_fee_cents  # noqa: E402

DB = os.path.join(ROOT, "sharp_edge.db")


def net_edge(our_prob, market_prob):
    """Percentage-point edge buying YES at market_prob, net of taker fee."""
    fee_pts = taker_fee_cents(int(round(market_prob)))  # cents/contract == pts
    return round(our_prob - market_prob - fee_pts, 2)


def log_signal(db_path, ticker, title, our_prob, market_prob):
    conn = sqlite3.connect(db_path)
    conn.execute(
        """INSERT INTO kalshi_signals
           (ts, market_ticker, title, our_prob, market_prob, edge_net_fees)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (datetime.now().isoformat(timespec="seconds"), ticker, title,
         our_prob, market_prob, net_edge(our_prob, market_prob)))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    series = sys.argv[1] if len(sys.argv) > 1 else None
    for m in get_markets(series_ticker=series):
        norm = normalize_market(m)
        if norm["market_prob"] is None:
            continue
        # Wiring our projection to a specific market requires parsing the
        # market title into (player, stat, line) — do this per-series once
        # the NBA series tickers are recorded in venues/kalshi.py (Task 15
        # step 4). Until then this logs market data only:
        print(norm["ticker"], norm["title"], norm["market_prob"])
