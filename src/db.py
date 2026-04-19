# src/db.py
import sqlite3
import pandas as pd
from datetime import datetime

DB_NAME = "sharp_edge.db"

def init_db():
    """Creates the predictions table if it doesn't exist."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # We store the player, the stat, the line, and default the status to PENDING
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            player TEXT,
            stat_type TEXT,
            line REAL,
            play TEXT,
            edge_percent REAL,
            actual_result REAL,
            status TEXT
        )
    ''')
    conn.commit()
    conn.close()
    print("[*] Database initialized successfully.")

def log_predictions(df):
    """Takes the premium plays DataFrame and logs them to the DB."""
    if df.empty:
        return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    today_date = datetime.now().strftime("%Y-%m-%d")
    
    # Filter for the exact same premium plays we send to Discord (>30% edge)
    premium_plays = df[df['Edge %'].abs() >= 30.0].head(5)
    
    inserted_count = 0
    for index, row in premium_plays.iterrows():
        # Check if we already logged this exact play today to avoid duplicates
        cursor.execute('''
            SELECT count(1) FROM predictions 
            WHERE date = ? AND player = ? AND stat_type = ?
        ''', (today_date, row['Player'], row['Stat']))
        
        exists = cursor.fetchone()[0]
        
        if not exists:
            cursor.execute('''
                INSERT INTO predictions (date, player, stat_type, line, play, edge_percent, status)
                VALUES (?, ?, ?, ?, ?, ?, 'PENDING')
            ''', (today_date, row['Player'], row['Stat'], row['PP Line'], row['Play'], row['Edge %']))
            inserted_count += 1

    conn.commit()
    conn.close()
    
    if inserted_count > 0:
        print(f"[+] Logged {inserted_count} new predictions to the database.")