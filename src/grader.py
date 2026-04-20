# src/grader.py
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from nba_fetcher import get_league_gamelog
from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import scoreboardv3, boxscoretraditionalv3
from utils.utils import logger, timer

DB_NAME = "sharp_edge.db"
NBA_TEAMS = teams.get_teams()
TEAM_DICT = {t['abbreviation']: t['id'] for t in NBA_TEAMS}

SCOREBOARD_CACHE = {}
BOXSCORE_CACHE = {} # NEW: Cache for live boxscores

@timer
def get_game_status(target_date_str, team_id):
    """Fetches game status and returns (is_final, game_state, game_id)."""
    if target_date_str not in SCOREBOARD_CACHE:
        try:
            time.sleep(0.5) 
            board = scoreboardv3.ScoreboardV3(game_date=target_date_str)
            lines_df = board.line_score.get_data_frame()
            headers_df = board.game_header.get_data_frame()
            SCOREBOARD_CACHE[target_date_str] = (lines_df, headers_df)
        except Exception as e:
            logger.error(f"Error fetching scoreboard for {target_date_str}: {e}")
            SCOREBOARD_CACHE[target_date_str] = (pd.DataFrame(), pd.DataFrame())

    lines_df, headers_df = SCOREBOARD_CACHE[target_date_str]
    
    if lines_df.empty or headers_df.empty:
        return False, "ERROR", None
        
    team_row = lines_df[lines_df['teamId'] == team_id]
    
    if team_row.empty:
        return False, "NO_GAME", None
        
    target_game_id = team_row.iloc[0]['gameId']
    game_row = headers_df[headers_df['gameId'] == target_game_id]
    
    if game_row.empty:
        return False, "NO_GAME", None
        
    status = game_row.iloc[0]['gameStatus']
    
    if status == 3:
        return True, "FINAL", target_game_id
    elif status == 2:
        return False, "LIVE", target_game_id
    else:
        return False, "PRE_GAME", target_game_id

@timer
def get_live_boxscore(game_id):
    """Fetches the live BoxScore for a specific game if it hasn't hit the historical logs yet."""
    if game_id not in BOXSCORE_CACHE:
        try:
            time.sleep(0.5)
            box = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
            df = box.player_stats.get_data_frame()
            BOXSCORE_CACHE[game_id] = df
        except Exception as e:
            logger.error(f"Error fetching live boxscore for {game_id}: {e}")
            BOXSCORE_CACHE[game_id] = pd.DataFrame()
            
    return BOXSCORE_CACHE[game_id]

def safe_float(val):
    if pd.isna(val) or val == '' or val is None:
         return 0.0
    return float(val)

@timer
def grade_pending_bets():
    logger.info("Booting up the Smart Grader (Live-Pivot Enabled)...")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("SELECT id, date, game_date, player, team, stat_type, line, play FROM predictions WHERE status = 'PENDING'")
    pending_bets = cursor.fetchall()

    if not pending_bets:
        logger.info("[-] No pending bets to grade. System is up to date.")
        conn.close()
        return

    logger.info(f"[*] Found {len(pending_bets)} pending bets. Fetching League-Wide Game Log...\n")

    league_df = get_league_gamelog()
    if league_df is not None and not league_df.empty:
        league_df['Parsed_Date'] = pd.to_datetime(league_df['GAME_DATE']).dt.strftime('%Y-%m-%d')
    else:
        logger.error("[!] Could not fetch league game logs. Exiting grader.")
        conn.close()
        return

    wins, losses, pushes, voids, skipped = 0, 0, 0, 0, 0
    today_date = datetime.now().strftime("%Y-%m-%d")
    graded_results = []

    for bet in pending_bets:
        bet_id, bet_date, game_date, player, team, stat_type, line, play = bet
        
        target_game_date = game_date if game_date else bet_date
        
        pp_to_nba_map = {
            "SA": "SAS", "NY": "NYK", "GS": "GSW", 
            "NO": "NOP", "UTAH": "UTA", "WSH": "WAS"
        }
        
        clean_team = pp_to_nba_map.get(team, team) if team else "UNK"
        team_id = TEAM_DICT.get(clean_team)

        # Fallback if team is missing
        if not team_id:
            player_logs = league_df[league_df['PLAYER_NAME'] == player]
            if not player_logs.empty:
                team_id = player_logs.iloc[0]['TEAM_ID']

        if not team_id:
            logger.warning(f"⚠️ {player} | Could not resolve Team ID. Leaving PENDING.")
            skipped += 1
            continue

        is_final, game_state, game_id = get_game_status(target_game_date, team_id)
            
        if not is_final:
            if game_state in ["LIVE", "PRE_GAME"]:
                logger.info(f"⏳ {player} | {stat_type} | Game is {game_state}. Leaving as PENDING.")
                skipped += 1
                continue
            else:
                if target_game_date < today_date:
                    # Date passed, game never happened
                    status = "VOID (DNP)"
                    actual = 0.0
                    voids += 1
                    cursor.execute('UPDATE predictions SET status = ?, actual_result = ? WHERE id = ?', (status, actual, bet_id))
                    logger.info(f"🔄 {player} | {stat_type} | {play} {line} -> Actual: {actual} ({status})")
                else:
                    logger.info(f"⏳ {player} | {stat_type} | Game hasn't started yet. Leaving PENDING.")
                    skipped += 1
                continue

        # === THE LIVE PIVOT LOGIC ===
        game_row = pd.DataFrame()
        is_historical = False
        
        # 1. Try checking the historical log first
        player_logs = league_df[league_df['PLAYER_NAME'] == player]
        if not player_logs.empty:
            historical_row = player_logs[player_logs['Parsed_Date'] == target_game_date]
            if not historical_row.empty:
                game_row = historical_row
                is_historical = True

        # 2. Pivot to the Live BoxScore if it's a same-day game
        if game_row.empty and game_id:
            box_df = get_live_boxscore(game_id)
            if not box_df.empty:
                # Lookup their ID locally using the static dictionary
                found_players = players.find_players_by_full_name(player)
                if found_players:
                    player_id = found_players[0]['id']
                    live_row = box_df[box_df['personId'] == player_id]
                    if not live_row.empty:
                        game_row = live_row
                        is_historical = False

        if game_row.empty:
            status = "VOID (DNP)" 
            actual = 0.0
            voids += 1
        else:
            # 3. Extract stats based on which API we pulled from
            if is_historical:
                pts = safe_float(game_row.iloc[0]['PTS'])
                reb = safe_float(game_row.iloc[0]['REB'])
                ast = safe_float(game_row.iloc[0]['AST'])
                fg3m = safe_float(game_row.iloc[0]['FG3M'])
                blk = safe_float(game_row.iloc[0]['BLK'])
                stl = safe_float(game_row.iloc[0]['STL'])
                tov = safe_float(game_row.iloc[0]['TOV'])
            else:
                # V3 Live API uses camelCase
                pts = safe_float(game_row.iloc[0].get('points', 0))
                reb = safe_float(game_row.iloc[0].get('reboundsTotal', 0))
                ast = safe_float(game_row.iloc[0].get('assists', 0))
                fg3m = safe_float(game_row.iloc[0].get('threePointersMade', 0))
                blk = safe_float(game_row.iloc[0].get('blocks', 0))
                stl = safe_float(game_row.iloc[0].get('steals', 0))
                tov = safe_float(game_row.iloc[0].get('turnovers', 0))
                
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
            logger.error(f"[-] Could not send Discord report: {e}")

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    grade_pending_bets()