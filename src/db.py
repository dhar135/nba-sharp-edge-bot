# src/db.py
import sqlite3
import pandas as pd
from datetime import datetime
from utils.utils import logger, timer

DB_NAME = "sharp_edge.db"

CORE_STATS = ["Points", "Rebounds", "Assists", "Pts+Rebs+Asts", "Pts+Rebs", "Pts+Asts", "Rebs+Asts"]
MICRO_STATS = ["3-PT Made", "Blocked Shots", "Steals", "Turnovers", "Blks+Stls"]

@timer
def init_db():
    """Creates the V2.0 predictions table."""
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
            v2_proj REAL,
            play TEXT,
            poisson_prob REAL,
            ev_edge REAL,
            vetoed INTEGER,
            ml_prob TEXT, 
            actual_result REAL,
            status TEXT
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("[*] V2 Database Schema initialized/verified successfully.")

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
        abs_edge = abs(row['EV Edge'])
        game_date = row.get('Game Date', today_date)
        
        cursor.execute('''
            SELECT ev_edge FROM predictions 
            WHERE date = ? AND player = ? AND stat_type = ?
        ''', (today_date, row['Player'], row['Stat']))
        
        existing_record = cursor.fetchone()
        
        if not existing_record:
            cursor.execute('''
                INSERT INTO predictions (date, game_date, player, team, matchup, stat_type, line, v2_proj, play, poisson_prob, ev_edge, vetoed, ml_prob, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')
            ''', (today_date, game_date, row['Player'], row['Team'], row['Matchup'], row['Stat'], row['PP Line'], row['V2 Proj'], row['Play'], row['Poisson Prob'], row['EV Edge'], int(row['Vetoed']), str(row['ML Prob'])))
            inserted_count += 1
        else:
            # === THE LINE MOVEMENT UPGRADE ===
            old_edge = abs(existing_record[0])
            # If the edge increased, overwrite the old record in the DB
            if abs_edge > old_edge:
                cursor.execute('''
                    UPDATE predictions 
                    SET line = ?, v2_proj = ?, poisson_prob = ?, ev_edge = ?, vetoed = ?, ml_prob = ?
                    WHERE date = ? AND player = ? AND stat_type = ?
                ''', (row['PP Line'], row['V2 Proj'], row['Poisson Prob'], row['EV Edge'], int(row['Vetoed']), str(row['ML Prob']), today_date, row['Player'], row['Stat']))
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
    
    def get_threshold(stat_list):
        placeholders = ','.join(['?'] * len(stat_list))
        query = f'''
            SELECT ABS(ev_edge) FROM predictions
            WHERE date = ? AND stat_type IN ({placeholders})
            ORDER BY ABS(ev_edge) DESC LIMIT 1 OFFSET 2
        '''
        cursor.execute(query, [today_date] + stat_list)
        res = cursor.fetchone()
        return res[0] if res else 2.5 # V2 threshold lowered to 2.5%

    core_threshold = get_threshold(CORE_STATS)
    micro_threshold = get_threshold(MICRO_STATS)
    
    new_rows = []
    for index, row in df.iterrows():
        abs_edge = abs(row['EV Edge'])
        
        # 1. Skip if Vetoed by ML
        if row['Vetoed']:
            continue
            
        # 2. The Podium Check
        threshold = core_threshold if row['Stat'] in CORE_STATS else micro_threshold
        if abs_edge <= threshold:
            continue 
                
        # 3. Has it been alerted today?
        cursor.execute('''
            SELECT ev_edge FROM predictions 
            WHERE date = ? AND player = ? AND stat_type = ?
        ''', (today_date, row['Player'], row['Stat']))
        existing_record = cursor.fetchone()
        
        if not existing_record:
            new_rows.append(row) 
        else:
            old_edge = abs(existing_record[0])
            if abs_edge >= (old_edge + 3.0):
                new_rows.append(row)
                
    conn.close()
    return pd.DataFrame(new_rows) if new_rows else pd.DataFrame()