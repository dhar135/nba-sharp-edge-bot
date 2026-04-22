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
            actual_result REAL,
            status TEXT
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("[*] Database initialized/verified successfully.")

@timer
def log_predictions(df):
    """Takes ALL generated plays and UPSERTS them to the DB."""
    if df.empty:
        return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    today_date = datetime.now().strftime("%Y-%m-%d")
    
    inserted_count = 0
    updated_count = 0
    
    for index, row in df.iterrows():
        abs_edge = abs(row['Edge %'])
        
        cursor.execute('''
            SELECT edge_percent FROM predictions 
            WHERE date = ? AND player = ? AND stat_type = ?
        ''', (today_date, row['Player'], row['Stat']))
        
        existing_record = cursor.fetchone()
        
        ml_prob_val = row.get('ML Prob', None)
        if ml_prob_val == "-" or pd.isna(ml_prob_val):
            ml_prob_val = None
            
        if not existing_record:
            cursor.execute('''
                INSERT INTO predictions (date, game_date, player, team, matchup, stat_type, line, play, edge_percent, ml_prob, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')
            ''', (today_date, row['Game Date'], row['Player'], row['Team'], row['Matchup'], row['Stat'], row['PP Line'], row['Play'], row['Edge %'], ml_prob_val))
            inserted_count += 1
        else:
            # === THE LINE MOVEMENT UPGRADE ===
            old_edge = abs(existing_record[0])
            # If the edge increased, overwrite the old record in the DB!
            if abs_edge > old_edge:
                cursor.execute('''
                    UPDATE predictions 
                    SET line = ?, edge_percent = ?, ml_prob = ?
                    WHERE date = ? AND player = ? AND stat_type = ?
                ''', (row['PP Line'], row['Edge %'], ml_prob_val, today_date, row['Player'], row['Stat']))
                updated_count += 1

    conn.commit()
    conn.close()
    
    if inserted_count > 0 or updated_count > 0:
        logger.info(f"[+] DB Update: {inserted_count} New Plays | {updated_count} Line Upgrades.")

def filter_new_plays(df):
    """Filters the dataframe to ONLY include plays that beat today's leaderboard."""
    if df.empty:
        return df
        
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    today_date = datetime.now().strftime("%Y-%m-%d")
    
    # Helper to find the "Score to beat" (The 3rd place edge currently in the DB)
    def get_threshold(stat_list):
        placeholders = ','.join(['?'] * len(stat_list))
        query = f'''
            SELECT ABS(edge_percent) FROM predictions
            WHERE date = ? AND stat_type IN ({placeholders})
            ORDER BY ABS(edge_percent) DESC LIMIT 1 OFFSET 2
        '''
        cursor.execute(query, [today_date] + stat_list)
        res = cursor.fetchone()
        return res[0] if res else 30.0 # Default to 30% if podium isn't full yet

    core_threshold = get_threshold(CORE_STATS)
    micro_threshold = get_threshold(MICRO_STATS)
    
    new_rows = []
    for index, row in df.iterrows():
        abs_edge = abs(row['Edge %'])
        
        # 1. AI Points Check (Always let high-confidence ML plays compete)
        is_ml_play = row['Stat'] == "Points" and pd.notna(row.get('ML Prob')) and row.get('ML Prob') != "-"
        
        # 2. The Podium Check
        if not is_ml_play:
            threshold = core_threshold if row['Stat'] in CORE_STATS else micro_threshold
            # If it doesn't beat the 3rd place play, ignore it!
            if abs_edge <= threshold:
                continue 
                
        # 3. Has it been alerted today?
        cursor.execute('''
            SELECT edge_percent FROM predictions 
            WHERE date = ? AND player = ? AND stat_type = ?
        ''', (today_date, row['Player'], row['Stat']))
        existing_record = cursor.fetchone()
        
        if not existing_record:
            new_rows.append(row) # It's a brand new top-tier play!
        else:
            old_edge = abs(existing_record[0])
            # Only send a RE-ALERT to Discord if the edge improved by at least 3%
            if abs_edge >= (old_edge + 3.0):
                new_rows.append(row)
                
    conn.close()
    return pd.DataFrame(new_rows) if new_rows else pd.DataFrame()
