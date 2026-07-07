"""Run near first game lock (cron): stamp pending predictions with the
current PrizePicks line so we can measure closing-line value (CLV)."""
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
DB = os.path.join(ROOT, "sharp_edge.db")


def update_closing_lines(board_df, db_path=DB):
    conn = sqlite3.connect(db_path)
    updated = 0
    for _, row in board_df.iterrows():
        cur = conn.execute(
            """UPDATE predictions SET closing_line = ?
               WHERE player = ? AND stat_type = ? AND game_date = ?
                 AND status = 'PENDING'""",
            (float(row["Line"]), row["Player"], row["Stat"], row.get("Game Date")))
        updated += cur.rowcount
    conn.commit()
    conn.close()
    return updated


if __name__ == "__main__":
    from extractors.pp_extractors import fetch_live_board
    from services.db import init_db
    init_db()  # ensure closing_line column exists
    board = fetch_live_board()
    if board.empty:
        print("Board empty — nothing to capture.")
    else:
        print(f"Updated {update_closing_lines(board)} pending rows with closing lines.")
