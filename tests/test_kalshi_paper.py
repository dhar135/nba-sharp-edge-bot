import sqlite3
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from kalshi_paper import net_edge, log_signal


def test_net_edge_subtracts_fee():
    # our prob 65%, market 57% -> gross 8 pts, minus taker fee at 57c
    e = net_edge(our_prob=65.0, market_prob=57.0)
    assert 5.0 < e < 8.0


def test_log_signal_inserts(tmp_db):
    log_signal(tmp_db, "KXTEST-1", "test market", 65.0, 57.0)
    n = sqlite3.connect(tmp_db).execute(
        "SELECT COUNT(*) FROM kalshi_signals").fetchone()[0]
    assert n == 1
