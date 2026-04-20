# src/grader.py
import os
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import time
from nba_fetcher import get_player_gamelog
from nba_api.stats.static import teams
from nba_api.stats.endpoints import scoreboardv3
from constants import calculate_actual

DB_NAME = "sharp_edge.db"

# 1. OPTIMIZATION: Build a static dictionary of Team Abbreviation -> Team ID
# This prevents us from having to ping the NBA API to find out who a player plays for.
NBA_TEAMS = teams.get_teams()
TEAM_DICT = {t['abbreviation']: t['id'] for t in NBA_TEAMS}

def is_game_final(target_date_str, team_id):
    """Checks the NBA scoreboard to see if the team's game on this date is FINAL."""
    try:
        board = scoreboardv3.ScoreboardV3(game_date=target_date_str)
        
        # 1. Fetch the specific dataframes we need
        lines_df = board.line_score.get_data_frame()
        headers_df = board.game_header.get_data_frame()
        
        # 2. Find the team's row in the LineScore table
        team_row = lines_df[lines_df['teamId'] == team_id]
        
        if team_row.empty:
            return False, "NO_GAME"
            
        # 3. Extract the gameId for that team's game
        target_game_id = team_row.iloc[0]['gameId']
        
        # 4. Look up that specific game in the GameHeader table
        game_row = headers_df[headers_df['gameId'] == target_game_id]
        
        if game_row.empty:
            return False, "NO_GAME"
            
        # 5. Extract the status (1 = PRE_GAME, 2 = LIVE, 3 = FINAL)
        status = game_row.iloc[0]['gameStatus']
        
        if status == 3:
            return True, "FINAL"
        elif status == 2:
            return False, "LIVE"
        else:
            return False, "PRE_GAME"
            
    except Exception as e:
        print(f"Error checking game status: {e}")
        return False, "ERROR"

def grade_pending_bets():
    print("[*] Booting up the Smart Grader (State-Aware)...")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # NEW: Fetch game_date from the DB
    cursor.execute("SELECT id, date, game_date, player, stat_type, line, play FROM predictions WHERE status = 'PENDING'")
    pending_bets = cursor.fetchall()

    if not pending_bets:
        print("[-] No pending bets to grade. System is up to date.")
        conn.close()
        return

    print(f"[*] Found {len(pending_bets)} pending bets. Verifying game statuses...\n")

    wins, losses, pushes, voids, skipped = 0, 0, 0, 0, 0
    today_date = datetime.now().strftime("%Y-%m-%d")

    unique_players = list(set([bet[3] for bet in pending_bets]))
    player_logs = {}
    team_mapping = {}

    # NEW: Create a list to track everything we grade today for the Discord alert
    graded_results = []

    # Map players to teams using their game logs
    for player in unique_players:
        df = get_player_gamelog(player)
        
        if df is not None and not df.empty:
            df['Parsed_Date'] = pd.to_datetime(df['GAME_DATE']).dt.strftime('%Y-%m-%d')
            player_logs[player] = df
            
            recent_matchup = df.iloc[0]['MATCHUP']
            team_abbr = recent_matchup[:3]
            team_mapping[player] = TEAM_DICT.get(team_abbr)
        else:
            team_mapping[player] = None
            
        time.sleep(1.2) 

    # Evaluate each bet
    for bet in pending_bets:
        bet_id, bet_date, game_date, player, stat_type, line, play = bet
        team_id = team_mapping.get(player)
        
        target_game_date = game_date if game_date else bet_date
        
        if not team_id:
            print(f"⚠️ {player} | Could not fetch valid game data. Leaving PENDING.")
            skipped += 1
            continue

        is_final, game_state = is_game_final(target_game_date, team_id)
            
        if not is_final:
            if game_state in ["LIVE", "PRE_GAME"]:
                print(f"⏳ {player} | {stat_type} | Game is {game_state}. Leaving as PENDING.")
                skipped += 1
                continue
            else:
                if target_game_date < today_date:
                    status = "VOID (DNP)"
                    actual = 0.0
                    voids += 1
                else:
                    print(f"⏳ {player} | {stat_type} | Game hasn't started yet. Leaving PENDING.")
                    skipped += 1
                    continue
        else:
            df = player_logs[player]
            game_row = df[df['Parsed_Date'] == target_game_date]
            
            if game_row.empty:
                status = "VOID (DNP)"
                actual = 0.0
                voids += 1
            else:
                actual = calculate_actual(stat_type, game_row)

                if actual == line:
                    status = "PUSH"
                    pushes += 1
                elif (play == "OVER" and actual > line) or (play == "UNDER" and actual < line):
                    status = "WIN"
                    wins += 1
                else:
                    status = "LOSS"
                    losses += 1

        # Execute DB Update
        cursor.execute('UPDATE predictions SET status = ?, actual_result = ? WHERE id = ?', (status, actual, bet_id))
        
        emoji = "✅" if status == "WIN" else "❌" if status == "LOSS" else "🔄"
        print(f"{emoji} {player} | {stat_type} | {play} {line} -> Actual: {actual} ({status})")
        
        # NEW: Append to our Discord tracking list
        graded_results.append({
            "player": player,
            "stat": stat_type,
            "play": play,
            "line": line,
            "actual": actual,
            "status": status
        })

    conn.commit()
    conn.close()
    
    total_graded = wins + losses
    win_rate = (wins / total_graded) * 100 if total_graded > 0 else 0.0
    
    print("\n=== DAILY GRADING REPORT ===")
    print(f"Wins:    {wins}")
    print(f"Losses:  {losses}")
    print(f"Pushes:  {pushes}")
    print(f"Voids:   {voids}")
    print(f"Skipped: {skipped} (Games pending)")
    if total_graded > 0:
         print(f"Win Rate: {win_rate:.1f}%")
         
    # NEW: Send the Discord Alert
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if webhook_url and graded_results:
        summary = {
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "voids": voids,
            "win_rate": win_rate
        }
        from notifier import send_grading_report
        send_grading_report(graded_results, summary, webhook_url)


# NEW: Load environment variables before executing
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()
    grade_pending_bets()