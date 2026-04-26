# src/services/db.py
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
    
    # Ensure the table has all V2 columns (graceful migration)
    try:
        cursor.execute("ALTER TABLE predictions ADD COLUMN v2_proj REAL")
        cursor.execute("ALTER TABLE predictions ADD COLUMN poisson_prob REAL")
        cursor.execute("ALTER TABLE predictions ADD COLUMN ev_edge REAL")
        cursor.execute("ALTER TABLE predictions ADD COLUMN vetoed INTEGER")
        conn.commit()
    except sqlite3.OperationalError:
        # Columns already exist
        pass
    
    conn.commit()
    conn.close()
    logger.info("[*] Database initialized/verified successfully with V2 schema.")

@timer
def log_predictions(df):
    """Takes ALL generated V2 plays and UPSERTS them to the DB."""
    if df.empty:
        return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    today_date = datetime.now().strftime("%Y-%m-%d")
    
    inserted_count = 0
    updated_count = 0
    
    for index, row in df.iterrows():
        # Support both V1 (Edge %) and V2 (EV Edge) column names
        ev_edge_val = row.get('EV Edge', row.get('Edge %', 0.0))
        
        cursor.execute('''
            SELECT ev_edge FROM predictions 
            WHERE date = ? AND player = ? AND stat_type = ?
        ''', (today_date, row['Player'], row['Stat']))
        
        existing_record = cursor.fetchone()
        
        ml_prob_val = row.get('ML Prob', None)
        if ml_prob_val == "-" or pd.isna(ml_prob_val):
            ml_prob_val = None
        
        # Extract V2 columns
        v2_proj = row.get('V2 Proj', None)
        poisson_prob = row.get('Poisson Prob', None)
        vetoed = 1 if row.get('Vetoed', False) else 0
        
        if not existing_record:
            cursor.execute('''
                INSERT INTO predictions (date, game_date, player, team, matchup, stat_type, line, play, edge_percent, ml_prob, v2_proj, poisson_prob, ev_edge, vetoed, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')
            ''', (today_date, row.get('Game Date'), row['Player'], row['Team'], row.get('Matchup'), row['Stat'], row.get('PP Line'), row.get('Play'), ev_edge_val, ml_prob_val, v2_proj, poisson_prob, ev_edge_val, vetoed))
            inserted_count += 1
        else:
            # Line movement upgrade: if EV edge increased, update the record
            old_ev_edge = existing_record[0] if existing_record[0] else 0.0
            if ev_edge_val > old_ev_edge:
                cursor.execute('''
                    UPDATE predictions 
                    SET line = ?, edge_percent = ?, ev_edge = ?, ml_prob = ?, v2_proj = ?, poisson_prob = ?, vetoed = ?
                    WHERE date = ? AND player = ? AND stat_type = ?
                ''', (row.get('PP Line'), ev_edge_val, ev_edge_val, ml_prob_val, v2_proj, poisson_prob, vetoed, today_date, row['Player'], row['Stat']))
                updated_count += 1

    conn.commit()
    conn.close()
    
    if inserted_count > 0 or updated_count > 0:
        logger.info(f"[+] DB Update: {inserted_count} New Plays | {updated_count} Line Upgrades.")

def filter_new_plays(df):
    """Filters the dataframe to ONLY include plays that are new or have improved."""
    if df.empty:
        return df
        
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    today_date = datetime.now().strftime("%Y-%m-%d")
    
    new_rows = []
    for index, row in df.iterrows():
        # Support both V1 and V2 edge columns
        current_ev_edge = row.get('EV Edge', row.get('Edge %', 0.0))
        
        # Check if this play exists in the database
        cursor.execute('''
            SELECT ev_edge, edge_percent FROM predictions 
            WHERE date = ? AND player = ? AND stat_type = ?
        ''', (today_date, row['Player'], row['Stat']))
        existing_record = cursor.fetchone()
        
        if not existing_record:
            # Brand new play - always alert
            new_rows.append(row)
            logger.info(f"    [+] NEW PLAY: {row['Player']} {row['Stat']}")
        else:
            # Play exists - only re-alert if EV edge improved by at least 0.5%
            old_ev_edge = existing_record[0] if existing_record[0] else 0.0
            if current_ev_edge >= (old_ev_edge + 0.5):
                new_rows.append(row)
                logger.info(f"    [+] UPGRADED: {row['Player']} {row['Stat']} (EV: {old_ev_edge:.2f}% → {current_ev_edge:.2f}%)")
                
    conn.close()
    return pd.DataFrame(new_rows) if new_rows else pd.DataFrame()
