# src/services/grader.py
"""
Live-Pivot Bet Grader: Scores pending predictions against actual game results
Uses both Live Boxscore (V2) and League Game Log (Fallback) for accuracy
"""
import sqlite3
import pandas as pd
import os
import json
import requests
from datetime import datetime
from nba_fetcher import get_league_gamelog, get_live_boxscore, get_game_status, resolve_stat_value
from services.notifier import send_grading_report
from utils.utils import logger, timer
from dotenv import load_dotenv

load_dotenv()
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")
DB_NAME = "sharp_edge.db"

@timer
def grade_pending_bets():
    logger.info("[INFO] Booting up the Smart Grader (Live-Pivot Enabled)...")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, player, team, stat_type, line, play, date, game_date, edge_percent, ml_prob 
        FROM predictions WHERE status = 'PENDING'
    ''')
    pending_bets = cursor.fetchall()
    
    if not pending_bets:
        logger.info("[INFO] [-] No pending bets found. Database is clean.")
        conn.close()
        return

    logger.info(f"[INFO] [*] Found {len(pending_bets)} pending bets. Fetching League-Wide Game Log...")
    league_df = get_league_gamelog()
    
    wins, losses, pushes, voids, skipped = 0, 0, 0, 0, 0
    graded_data = [] 
    
    game_status_cache = {}
    boxscore_cache = {}

    for bet in pending_bets:
        bet_id, player, team, stat_type, line, play, date, game_date, edge, ml_prob = bet
        actual_val = None
        result_status = "PENDING"
        
        # FIX: Fallback to the scrape date if game_date is missing or None
        target_date = game_date if game_date else date
        
        # Use target_date instead of game_date
        status = get_game_status(player, target_date, game_status_cache, team)
                
        logger.info(f"[DEBUG] {player} on {target_date} | Team: {team} | API Status: {status}")
        
        if status in ["PRE_GAME", "IN_PROGRESS"]:
            skipped += 1
            logger.info(f"[INFO] ⏳ {player} | {stat_type} | Game is {status}. Leaving as PENDING.")
            continue
            
        elif status == "VOID (DNP)":
            result_status = "VOID"
            voids += 1
            logger.info(f"[INFO] 🚫 {player} | {stat_type} | Did Not Play (VOID).")
            
        elif status == "FINAL":
            game_id = game_status_cache[player]['game_id']

            if game_id not in boxscore_cache:
                live_df = get_live_boxscore(game_id)
                boxscore_cache[game_id] = live_df

            box_df = boxscore_cache[game_id]
            if box_df is not None and not box_df.empty:
                player_box = box_df[box_df['PLAYER_ID'] == game_status_cache[player]['player_id']]
                
                if not player_box.empty:
                    pb = player_box.iloc[0]
                    try:
                        actual_val = resolve_stat_value(pb, stat_type)
                    except Exception:
                        actual_val = None

            if actual_val is None and league_df is not None:
                player_logs = league_df[(league_df['PLAYER_NAME'] == player) & (league_df['GAME_DATE'] == game_date)]
                if not player_logs.empty:
                    pb = player_logs.iloc[0]
                    actual_val = resolve_stat_value(pb, stat_type)

            if actual_val is not None:
                if (play == "OVER" and actual_val > line) or (play == "UNDER" and actual_val < line):
                    result_status = "WIN"
                    wins += 1
                    logger.info(f"[INFO] ✅ {player} | {stat_type} | {play} {line} -> Actual: {actual_val} (WIN)")
                elif actual_val == line:
                    result_status = "PUSH"
                    pushes += 1
                    logger.info(f"[INFO] 🟰 {player} | {stat_type} | {play} {line} -> Actual: {actual_val} (PUSH)")
                else:
                    result_status = "LOSS"
                    losses += 1
                    logger.info(f"[INFO] ❌ {player} | {stat_type} | {play} {line} -> Actual: {actual_val} (LOSS)")
            else:
                if status == "FINAL":
                    result_status = "VOID (DNP)"
                    voids += 1
                    logger.info(f"[INFO] 🚫 {player} | {stat_type} | Game Final but no stats found (DNP).")
                else:
                    skipped += 1
                    logger.info(f"[INFO] ⏳ {player} | {stat_type} | Stats not populated yet. Leaving as PENDING.")
                    continue

        cursor.execute('''
            UPDATE predictions 
            SET status = ?, actual_result = ? 
            WHERE id = ?
        ''', (result_status, actual_val, bet_id))
        
        if result_status != "PENDING":
            graded_data.append({
                "Player": player,
                "Stat": stat_type,
                "Line": line,
                "Play": play,
                "Actual": actual_val,
                "Result": result_status,
                "Edge %": edge,
                "ML Prob": ml_prob
            })

    conn.commit()
    conn.close()

    total_decisions = wins + losses
    win_rate = (wins / total_decisions * 100) if total_decisions > 0 else 0.0

    logger.info("\n=== DAILY GRADING REPORT ===")
    logger.info(f"[INFO] Wins:    {wins}")
    logger.info(f"[INFO] Losses:  {losses}")
    logger.info(f"[INFO] Pushes:  {pushes}")
    logger.info(f"[INFO] Voids:   {voids}")
    logger.info(f"[INFO] Skipped: {skipped} (Games pending)")
    if total_decisions > 0:
        logger.info(f"[INFO] Win Rate: {win_rate:.1f}%")

    if wins > 0 or losses > 0 or pushes > 0 or voids > 0:
        csv_path = None
        if graded_data:
            out_dir = "data"
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)
            csv_path = os.path.join(out_dir, f"graded_results_{datetime.now().strftime('%Y%m%d')}.csv")
            pd.DataFrame(graded_data).to_csv(csv_path, index=False)

        summary = {
            "wins": wins, "losses": losses, "pushes": pushes, "voids": voids, 
            "win_rate": round(win_rate, 1), "skipped": skipped
        }
        send_grading_report(summary, csv_path, DISCORD_WEBHOOK)

if __name__ == "__main__":
    grade_pending_bets()