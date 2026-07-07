"""Replay strategy configs against graded history.
LIMITATION: only tightens filters over logged plays; it cannot surface
plays the production filters excluded (those were never logged).
Usage: python scripts/backtest.py"""
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from season_report import compute_flex_roi  # noqa: E402


def load_rows(db_path=os.path.join(ROOT, "sharp_edge.db")):
    return sqlite3.connect(db_path).execute(
        "SELECT stat_type, play, ev_edge, player, vegas_confirms, status "
        "FROM predictions WHERE status IN ('WIN','LOSS')").fetchall()


def backtest(rows, evaluator):
    kept = [r for r in rows
            if evaluator(r[0], r[1], r[2] or 0.0, r[3],
                         None if r[4] is None else bool(r[4]))]
    n = len(kept)
    wins = sum(1 for r in kept if r[5] == "WIN")
    wr = round(100.0 * wins / n, 1) if n else 0.0
    return {"n": n, "wins": wins, "win_rate": wr,
            "roi_flex5": compute_flex_roi(wr / 100.0, "flex5") if n else 0.0}


if __name__ == "__main__":
    from engine.strategy import evaluate_play
    rows = load_rows()

    def current(stat, play, edge, player, vegas):
        return evaluate_play(stat, play, edge, player_name=player,
                             vegas_confirms=vegas)[0]

    baseline = {"n": len(rows),
                "wins": sum(1 for r in rows if r[5] == "WIN")}
    baseline["win_rate"] = round(100.0 * baseline["wins"] / baseline["n"], 1)
    print(f"All logged bets:      {baseline}")
    print(f"Current strategy:     {backtest(rows, current)}")
