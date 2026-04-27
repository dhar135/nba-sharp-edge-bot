# src/services/db.py
"""
V2.1 Database Service — SQLite persistence with deduplication

Key changes from V2.0:
  1. Proper deduplication: uses (date, player, stat_type, game_date) as unique key
  2. New columns: confidence score, strategy tier
  3. Vetoed plays are never logged (enforced at main.py level, but double-checked here)
"""
import sqlite3
import pandas as pd
from datetime import datetime
from utils.utils import logger, timer

DB_NAME = "sharp_edge.db"

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
    ]
    for col_name, col_type in new_columns:
        try:
            cursor.execute(f"ALTER TABLE predictions ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            pass  # Column already exists

    conn.commit()
    conn.close()
    logger.info("[*] Database initialized/verified successfully with V2.1 schema.")

@timer
def log_predictions(df):
    """
    Takes generated V2.1 plays and UPSERTS them to the DB.
    Enforces deduplication using (date, player, stat_type, game_date) as the unique key.
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

        # DEDUP CHECK: Use (date, player, stat_type, game_date) for uniqueness
        cursor.execute('''
            SELECT id, ev_edge FROM predictions
            WHERE date = ? AND player = ? AND stat_type = ? AND (game_date = ? OR (game_date IS NULL AND ? IS NULL))
        ''', (today_date, row['Player'], row['Stat'], game_date, game_date))

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

        # Double-check: NEVER log vetoed plays
        if vetoed:
            continue

        if not existing_record:
            cursor.execute('''
                INSERT INTO predictions
                (date, game_date, player, team, matchup, stat_type, line, play,
                 edge_percent, ml_prob, v2_proj, poisson_prob, ev_edge, vetoed,
                 confidence, tier, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')
            ''', (today_date, game_date, row['Player'], row['Team'],
                  row.get('Matchup'), row['Stat'], row.get('PP Line'),
                  row.get('Play'), ev_edge_val, ml_prob_val,
                  v2_proj, poisson_prob, ev_edge_val, vetoed,
                  confidence, tier))
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
                        confidence = ?, tier = ?
                    WHERE id = ?
                ''', (row.get('PP Line'), ev_edge_val, ev_edge_val, ml_prob_val,
                      v2_proj, poisson_prob, vetoed, confidence, tier, existing_id))
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
    Uses (date, player, stat_type, game_date) for uniqueness to prevent dupes.
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

        # Check for existing play using full dedup key
        cursor.execute('''
            SELECT ev_edge, edge_percent FROM predictions
            WHERE date = ? AND player = ? AND stat_type = ? AND (game_date = ? OR (game_date IS NULL AND ? IS NULL))
        ''', (today_date, row['Player'], row['Stat'], game_date, game_date))
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
