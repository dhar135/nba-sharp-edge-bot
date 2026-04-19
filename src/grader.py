# src/grader.py
import sqlite3
import pandas as pd
from datetime import datetime
import time
from nba_fetcher import get_player_gamelog

DB_NAME = "sharp_edge.db"

def grade_pending_bets():
    # Check if it's too early to grade (e.g., if you boot up at 2 AM before games end)
    current_hour = datetime.now().hour
    if current_hour < 7: # Don't grade before 7 AM
        print("[-] It's too early to grade yesterday's games. Waiting for morning.")
        return
    
    print("[*] Booting up the Sharp Edge Grader...")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # 1. Fetch all PENDING bets from the database
    cursor.execute("SELECT id, date, player, stat_type, line, play FROM predictions WHERE status = 'PENDING'")
    pending_bets = cursor.fetchall()

    if not pending_bets:
        print("[-] No pending bets to grade. System is up to date.")
        conn.close()
        return

    print(f"[*] Found {len(pending_bets)} pending bets. Fetching official box scores...\n")

    wins = 0
    losses = 0
    pushes = 0
    voids = 0

    # OPTIMIZATION: Get a list of unique players so we only hit the NBA API once per player
    unique_players = list(set([bet[2] for bet in pending_bets]))
    player_logs = {}

    for player in unique_players:
        df = get_player_gamelog(player)
        if df is not None and not df.empty:
            # The NBA API returns dates like 'APR 18, 2026'. We convert it to 'YYYY-MM-DD' to match our DB.
            df['Parsed_Date'] = pd.to_datetime(df['GAME_DATE']).dt.strftime('%Y-%m-%d')
            player_logs[player] = df
        time.sleep(0.6) # Respect rate limits

    # 2. Evaluate each bet
    for bet in pending_bets:
        bet_id, bet_date, player, stat_type, line, play = bet
        
        # If the player wasn't found in the API at all
        if player not in player_logs:
            status = "VOID (No Data)"
            actual = 0.0
            voids += 1
            continue
            
        df = player_logs[player]
        
        # Look for a game log matching the exact date the bet was logged
        game_row = df[df['Parsed_Date'] == bet_date]
        
        if game_row.empty:
            status = "VOID (DNP)" # Did Not Play (Injured, benched, or game postponed)
            actual = 0.0
            voids += 1
        else:
            # Extract the actual stats from the box score
            pts = float(game_row.iloc[0]['PTS'])
            reb = float(game_row.iloc[0]['REB'])
            ast = float(game_row.iloc[0]['AST'])
            
            if stat_type == "Points":
                actual = pts
            elif stat_type == "Rebounds":
                actual = reb
            elif stat_type == "Assists":
                actual = ast
            elif stat_type == "Pts+Rebs+Asts":
                actual = pts + reb + ast
            else:
                actual = 0.0

            # The Grading Logic
            if actual == line:
                status = "PUSH"
                pushes += 1
            elif (play == "OVER" and actual > line) or (play == "UNDER" and actual < line):
                status = "WIN"
                wins += 1
            else:
                status = "LOSS"
                losses += 1

        # 3. Update the Database with the final result
        cursor.execute('''
            UPDATE predictions 
            SET status = ?, actual_result = ?
            WHERE id = ?
        ''', (status, actual, bet_id))
        
        # Print a clean terminal output
        emoji = "✅" if status == "WIN" else "❌" if status == "LOSS" else "🔄"
        print(f"{emoji} {player} | {stat_type} | {play} {line} -> Actual: {actual} ({status})")

    # Save changes and close DB
    conn.commit()
    conn.close()
    
    print("\n=== DAILY GRADING REPORT ===")
    print(f"Wins:   {wins}")
    print(f"Losses: {losses}")
    print(f"Pushes: {pushes}")
    print(f"Voids:  {voids}")
    
    # Calculate ROI (Assuming standard -110 juice equivalent for math tracking)
    total_graded = wins + losses
    if total_graded > 0:
        win_rate = (wins / total_graded) * 100
        print(f"Win Rate: {win_rate:.1f}%")

if __name__ == "__main__":
    grade_pending_bets()