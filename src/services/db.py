# src/services/db.py
"""
V2.1 Database Service — SQLite persistence with deduplication

Key changes from V2.0:
  1. Proper deduplication: uses (game_date, player, stat_type) as unique key
     (run date is NOT part of the key — a play logged on two different run
     days for the same game is the same bet, not two bets)
  2. New columns: confidence score, strategy tier
  3. Vetoed plays are never logged (enforced at main.py level, but double-checked here)
"""
import sqlite3
import pandas as pd
from datetime import datetime
from utils.utils import logger, timer

import os

# Always resolve the DB path relative to the project root (two levels up from this file)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_NAME = os.path.join(_PROJECT_ROOT, "sharp_edge.db")

CORE_STATS = ["Points", "Rebounds", "Assists", "Pts+Rebs+Asts", "Pts+Rebs", "Pts+Asts", "Rebs+Asts"]
MICRO_STATS = ["3-PT Made", "Blocked Shots", "Steals", "Turnovers", "Blks+Stls"]

@timer
def init_db():
    """Creates the predictions table if it doesn't exist."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            game_date TEXT,
            player TEXT,
            team TEXT,
            matchup TEXT,
            stat_type TEXT,
            line REAL,
            play TEXT,
            edge_percent REAL,
            ml_prob REAL,
            v2_proj REAL,
            poisson_prob REAL,
            ev_edge REAL,
            vetoed INTEGER,
            actual_result REAL,
            status TEXT
        )
    ''')

    # Ensure the table has all V2.1 columns (graceful migration)
    new_columns = [
        ("v2_proj", "REAL"),
        ("poisson_prob", "REAL"),
        ("ev_edge", "REAL"),
        ("vetoed", "INTEGER"),
        ("confidence", "REAL"),
        ("tier", "TEXT"),
        ("vegas_line", "REAL"),       # Median Vegas consensus line
        ("vegas_confirms", "INTEGER"), # 1 = Vegas agrees with our play direction
        ("closing_line", "REAL"),     # PrizePicks line captured near first lock (for CLV)
    ]
    for col_name, col_type in new_columns:
        try:
            cursor.execute(f"ALTER TABLE predictions ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            pass  # Column already exists

    try:
        cursor.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_pred_unique
                          ON predictions(game_date, player, stat_type)""")
    except sqlite3.OperationalError:
        pass  # legacy DB still has duplicates — run scripts/dedupe_db.py

    conn.commit()
    conn.close()
    logger.info("[*] Database initialized/verified successfully with V2.1 schema.")

@timer
def log_predictions(df):
    """
    Takes generated V2.1 plays and UPSERTS them to the DB.
    Enforces deduplication using (game_date, player, stat_type) as the unique key.
    """
    if df.empty:
        return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    today_date = datetime.now().strftime("%Y-%m-%d")

    inserted_count = 0
    updated_count = 0
    dedup_count = 0

    for index, row in df.iterrows():
        ev_edge_val = row.get('EV Edge', row.get('Edge %', 0.0))
        game_date = row.get('Game Date', None)

        # DEDUP KEY: the bet's identity is (game_date, player, stat_type).
        # Run date must NOT be part of the key (caused season-long double logging).
        dedup_game_date = game_date or today_date
        cursor.execute('''
            SELECT id, ev_edge FROM predictions
            WHERE game_date = ? AND player = ? AND stat_type = ?
        ''', (dedup_game_date, row['Player'], row['Stat']))

        existing_record = cursor.fetchone()

        ml_prob_val = row.get('ML Prob', None)
        if ml_prob_val == "-" or (ml_prob_val is not None and pd.isna(ml_prob_val)):
            ml_prob_val = None

        # Extract V2.1 columns
        v2_proj = row.get('V2 Proj', None)
        poisson_prob = row.get('Poisson Prob', None)
        vetoed = 1 if row.get('Vetoed', False) else 0
        confidence = row.get('Confidence', None)
        tier = row.get('Tier', None)
        vegas_line = row.get('Vegas Line', None)
        vegas_confirms_raw = row.get('Vegas Confirms', None)
        vegas_confirms = None if vegas_confirms_raw is None else (1 if vegas_confirms_raw else 0)

        # Double-check: NEVER log vetoed plays
        if vetoed:
            continue

        if not existing_record:
            cursor.execute('''
                INSERT INTO predictions
                (date, game_date, player, team, matchup, stat_type, line, play,
                 edge_percent, ml_prob, v2_proj, poisson_prob, ev_edge, vetoed,
                 confidence, tier, vegas_line, vegas_confirms, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')
            ''', (today_date, dedup_game_date, row['Player'], row['Team'],
                  row.get('Matchup'), row['Stat'], row.get('PP Line'),
                  row.get('Play'), ev_edge_val, ml_prob_val,
                  v2_proj, poisson_prob, ev_edge_val, vetoed,
                  confidence, tier, vegas_line, vegas_confirms))
            inserted_count += 1
        else:
            existing_id, old_ev_edge = existing_record
            old_ev_edge = old_ev_edge if old_ev_edge else 0.0

            # Only update if the edge improved meaningfully (prevents duplicate noise)
            if ev_edge_val > (old_ev_edge + 0.5):
                cursor.execute('''
                    UPDATE predictions
                    SET line = ?, edge_percent = ?, ev_edge = ?, ml_prob = ?,
                        v2_proj = ?, poisson_prob = ?, vetoed = ?,
                        confidence = ?, tier = ?,
                        vegas_line = ?, vegas_confirms = ?
                    WHERE id = ?
                ''', (row.get('PP Line'), ev_edge_val, ev_edge_val, ml_prob_val,
                      v2_proj, poisson_prob, vetoed, confidence, tier,
                      vegas_line, vegas_confirms, existing_id))
                updated_count += 1
            else:
                dedup_count += 1

    conn.commit()
    conn.close()

    if inserted_count > 0 or updated_count > 0:
        logger.info(f"[+] DB Update: {inserted_count} New | {updated_count} Upgraded | {dedup_count} Deduped")


def filter_new_plays(df):
    """
    Filters the dataframe to ONLY include plays that are new or have improved.
    Uses (game_date, player, stat_type) for uniqueness to prevent dupes.
    """
    if df.empty:
        return df

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    today_date = datetime.now().strftime("%Y-%m-%d")

    new_rows = []
    for index, row in df.iterrows():
        current_ev_edge = row.get('EV Edge', row.get('Edge %', 0.0))
        game_date = row.get('Game Date', None)

        # DEDUP KEY: the bet's identity is (game_date, player, stat_type).
        # Run date must NOT be part of the key (caused season-long double logging).
        dedup_game_date = game_date or today_date
        cursor.execute('''
            SELECT ev_edge, edge_percent FROM predictions
            WHERE game_date = ? AND player = ? AND stat_type = ?
        ''', (dedup_game_date, row['Player'], row['Stat']))
        existing_record = cursor.fetchone()

        if not existing_record:
            new_rows.append(row)
            logger.info(f"    [+] NEW PLAY: {row['Player']} {row['Stat']}")
        else:
            old_ev_edge = existing_record[0] if existing_record[0] else 0.0
            if current_ev_edge >= (old_ev_edge + 0.5):
                new_rows.append(row)
                logger.info(f"    [+] UPGRADED: {row['Player']} {row['Stat']} (EV: {old_ev_edge:.2f}% → {current_ev_edge:.2f}%)")

    conn.close()
    return pd.DataFrame(new_rows) if new_rows else pd.DataFrame()
