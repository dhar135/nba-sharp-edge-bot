"""One-time cleanup: collapse duplicate predictions created by the old
run-date dedup key. Keeps the earliest row (MIN id) per bet identity.
Backs up the DB to sharp_edge.db.bak first."""
import os
import shutil
import sqlite3

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "sharp_edge.db")

shutil.copy(DB, DB + ".bak")
print(f"Backup written: {DB}.bak")

conn = sqlite3.connect(DB)
before = conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]

conn.execute("UPDATE predictions SET game_date = date WHERE game_date IS NULL")
conn.execute("""
    DELETE FROM predictions WHERE id NOT IN (
        SELECT MIN(id) FROM predictions
        GROUP BY game_date, player, stat_type
    )
""")
conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_pred_unique
                ON predictions(game_date, player, stat_type)""")
conn.commit()

after = conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
graded = conn.execute(
    "SELECT COUNT(*), ROUND(100.0*SUM(status='WIN')/COUNT(*),1) "
    "FROM predictions WHERE status IN ('WIN','LOSS')").fetchone()
print(f"Rows: {before} -> {after} (removed {before - after})")
print(f"Graded unique bets: {graded[0]} | Win rate: {graded[1]}%")
