import sqlite3
import sys, os
import pandas as pd
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from capture_closing_lines import update_closing_lines


def test_updates_pending_rows_only(tmp_db):
    conn = sqlite3.connect(tmp_db)
    conn.execute("""INSERT INTO predictions
        (game_date, player, stat_type, line, play, status)
        VALUES ('2026-11-01', 'Test Guy', 'Points', 20.5, 'UNDER', 'PENDING')""")
    conn.execute("""INSERT INTO predictions
        (game_date, player, stat_type, line, play, status)
        VALUES ('2026-10-30', 'Old Guy', 'Points', 11.5, 'UNDER', 'WIN')""")
    conn.commit()

    board = pd.DataFrame([
        {"Player": "Test Guy", "Stat": "Points", "Line": 19.5, "Game Date": "2026-11-01"},
        {"Player": "Old Guy", "Stat": "Points", "Line": 12.5, "Game Date": "2026-10-30"},
    ])
    updated = update_closing_lines(board, tmp_db)
    assert updated == 1
    row = conn.execute("SELECT closing_line FROM predictions "
                       "WHERE player='Test Guy'").fetchone()
    assert row[0] == 19.5
