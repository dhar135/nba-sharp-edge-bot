# src/grader.py
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from nba_fetcher import get_league_gamelog
from nba_api.stats.static import teams
from nba_api.stats.endpoints import scoreboardv3
from utils import logger, timer

DB_NAME = "sharp_edge.db"
NBA_TEAMS = teams.get_teams()
TEAM_DICT = {t['abbreviation']: t['id'] for t in NBA_TEAMS}

# NEW: Cache the scoreboard so we don't fetch the same date 20 times
SCOREBOARD_CACHE = {}

@timer
def get_game_status(target_date_str, team_id):
    """Fetches game status using an in-memory cache to prevent rate limits."""
    if target_date_str not in SCOREBOARD_CACHE:
        try:
            time.sleep(0.5) # Polite buffer for the API
            board = scoreboardv3.ScoreboardV3(game_date=target_date_str)
            lines_df = board.line_score.get_data_frame()
            headers_df = board.game_header.get_data_frame()
            SCOREBOARD_CACHE[target_date_str] = (lines_df, headers_df)
        except Exception as e:
            logger.info(f"Error fetching scoreboard for {target_date_str}: {e}")
            SCOREBOARD_CACHE[target_date_str] = (pd.DataFrame(), pd.DataFrame())

    lines_df, headers_df = SCOREBOARD_CACHE[target_date_str]
    
    if lines_df.empty or headers_df.empty:
        return False, "ERROR"
        
    team_row = lines_df[lines_df['teamId'] == team_id]
    
    if team_row.empty:
        return False, "NO_GAME"
        
    target_game_id = team_row.iloc[0]['gameId']
    game_row = headers_df[headers_df['gameId'] == target_game_id]
    
    if game_row.empty:
        return False, "NO_GAME"
        
    status = game_row.iloc[0]['gameStatus']
    
    if status == 3:
        return True, "FINAL"
    elif status == 2:
        return False, "LIVE"
    else:
        return False, "PRE_GAME"

@timer
def grade_pending_bets():
    logger.info("[*] Booting up the Smart Grader (God-Call Optimized)...")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # We now fetch the 'team' directly from the DB to skip API lookups!
    cursor.execute("SELECT id, date, game_date, player, team, stat_type, line, play FROM predictions WHERE status = 'PENDING'")
    pending_bets = cursor.fetchall()

    if not pending_bets:
        logger.info("[-] No pending bets to grade. System is up to date.")
        conn.close()
        return

    logger.info(f"[*] Found {len(pending_bets)} pending bets. Fetching League-Wide Game Log...\n")

    # 1. THE GOD CALL
    league_df = get_league_gamelog()
    if league_df is not None and not league_df.empty:
        league_df['Parsed_Date'] = pd.to_datetime(league_df['GAME_DATE']).dt.strftime('%Y-%m-%d')
    else:
        logger.info("[!] Could not fetch league game logs. Exiting grader.")
        conn.close()
        return

    wins, losses, pushes, voids, skipped = 0, 0, 0, 0, 0
    today_date = datetime.now().strftime("%Y-%m-%d")
    graded_results = []

    # 2. Evaluate each bet entirely in memory!
    for bet in pending_bets:
        bet_id, bet_date, game_date, player, team, stat_type, line, play = bet
        
        target_game_date = game_date if game_date else bet_date
        
        # Instantly resolve the Team ID from our static dictionary
        team_id = TEAM_DICT.get(team)

        if not team_id:
            logger.info(f"⚠️ {player} | Could not resolve Team ID for '{team}'. Leaving PENDING.")
            skipped += 1
            continue

        is_final, game_state = get_game_status(target_game_date, team_id)
            
        if not is_final:
            if game_state in ["LIVE", "PRE_GAME"]:
                logger.info(f"⏳ {player} | {stat_type} | Game is {game_state}. Leaving as PENDING.")
                skipped += 1
                continue
            else:
                if target_game_date < today_date:
                    status = "VOID (DNP)"
                    actual = 0.0
                    voids += 1
                else:
                    logger.info(f"⏳ {player} | {stat_type} | Game hasn't started yet. Leaving PENDING.")
                    skipped += 1
                    continue
        else:
            # Game is final, slice the specific player's box score from memory
            player_logs = league_df[league_df['PLAYER_NAME'] == player]
            game_row = player_logs[player_logs['Parsed_Date'] == target_game_date]
            
            if game_row.empty:
                status = "VOID (DNP)" 
                actual = 0.0
                voids += 1
            else:
                pts = float(game_row.iloc[0]['PTS'])
                reb = float(game_row.iloc[0]['REB'])
                ast = float(game_row.iloc[0]['AST'])
                fg3m = float(game_row.iloc[0]['FG3M'])
                blk = float(game_row.iloc[0]['BLK'])
                stl = float(game_row.iloc[0]['STL'])
                tov = float(game_row.iloc[0]['TOV'])
                
                if stat_type == "Points": actual = pts
                elif stat_type == "Rebounds": actual = reb
                elif stat_type == "Assists": actual = ast
                elif stat_type == "Pts+Rebs+Asts": actual = pts + reb + ast
                elif stat_type == "3-PT Made": actual = fg3m
                elif stat_type == "Blocked Shots": actual = blk
                elif stat_type == "Steals": actual = stl
                elif stat_type == "Turnovers": actual = tov
                elif stat_type == "Blks+Stls": actual = blk + stl
                elif stat_type == "Pts+Rebs": actual = pts + reb
                elif stat_type == "Pts+Asts": actual = pts + ast
                elif stat_type == "Rebs+Asts": actual = reb + ast
                else: actual = 0.0

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
        logger.info(f"{emoji} {player} | {stat_type} | {play} {line} -> Actual: {actual} ({status})")
        
        if status not in ["VOID (DNP)"]:
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
    
    logger.info("\n=== DAILY GRADING REPORT ===")
    logger.info(f"Wins:    {wins}")
    logger.info(f"Losses:  {losses}")
    logger.info(f"Pushes:  {pushes}")
    logger.info(f"Voids:   {voids}")
    logger.info(f"Skipped: {skipped} (Games pending)")
    if total_graded > 0:
         logger.info(f"Win Rate: {win_rate:.1f}%")
         
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if webhook_url and graded_results:
        summary = {
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "voids": voids,
            "win_rate": win_rate
        }
        try:
            from notifier import send_grading_report
            send_grading_report(graded_results, summary, webhook_url)
        except Exception as e:
            logger.info(f"[-] Could not send Discord report: {e}")

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    grade_pending_bets()