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
from nba_fetcher import get_league_gamelog, get_live_boxscore, get_game_status
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
        
        status = get_game_status(player, game_date, game_status_cache, team)
        
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
                        if stat_type == "Points": actual_val = pb['PTS']
                        elif stat_type == "Rebounds": actual_val = pb['REB']
                        elif stat_type == "Assists": actual_val = pb['AST']
                        elif stat_type == "Pts+Rebs+Asts": actual_val = pb['PTS'] + pb['REB'] + pb['AST']
                        elif stat_type == "Pts+Rebs": actual_val = pb['PTS'] + pb['REB']
                        elif stat_type == "Pts+Asts": actual_val = pb['PTS'] + pb['AST']
                        elif stat_type == "Rebs+Asts": actual_val = pb['REB'] + pb['AST']
                        elif stat_type == "3-PT Made": actual_val = pb['FG3M']
                        elif stat_type == "Blocked Shots": actual_val = pb['BLK']
                        elif stat_type == "Steals": actual_val = pb['STL']
                        elif stat_type == "Turnovers": actual_val = pb['TOV']
                        elif stat_type == "Blks+Stls": actual_val = pb['BLK'] + pb['STL']
                    except Exception:
                        actual_val = None

            if actual_val is None and league_df is not None:
                player_logs = league_df[(league_df['PLAYER_NAME'] == player) & (league_df['GAME_DATE'] == game_date)]
                if not player_logs.empty:
                    pb = player_logs.iloc[0]
                    if stat_type == "Points": actual_val = pb['PTS']
                    elif stat_type == "Rebounds": actual_val = pb['REB']
                    elif stat_type == "Assists": actual_val = pb['AST']
                    elif stat_type == "Pts+Rebs+Asts": actual_val = pb['PTS'] + pb['REB'] + pb['AST']
                    elif stat_type == "Pts+Rebs": actual_val = pb['PTS'] + pb['REB']
                    elif stat_type == "Pts+Asts": actual_val = pb['PTS'] + pb['AST']
                    elif stat_type == "Rebs+Asts": actual_val = pb['REB'] + pb['AST']
                    elif stat_type == "3-PT Made": actual_val = pb['FG3M']
                    elif stat_type == "Blocked Shots": actual_val = pb['BLK']
                    elif stat_type == "Steals": actual_val = pb['STL']
                    elif stat_type == "Turnovers": actual_val = pb['TOV']
                    elif stat_type == "Blks+Stls": actual_val = pb['BLK'] + pb['STL']

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

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return ""

    client = genai.Client(api_key=api_key)

    # Updated Prompt for Gemini 3 Reasoning
    prompt = (
        "You are an elite NBA quantitative analyst. I have identified mathematical edges on PrizePicks. "
        "Use GOOGLE SEARCH to check for the latest injury news or lineup changes from the last 12 hours for these players. "
        "Provide ONE punchy sentence per player explaining the situational edge. Keep it brief (under 20 words).\n\n"
    )

    for index, row in plays_df.iterrows():
        prompt += f"- {row['Player']} ({row['Team']}) vs {row['Matchup']}. Bet: {row['Play']} {row['PP Line']} {row['Stat']}.\n"

    try:
        logger.info("[*] Contacting Gemini 3.1 Flash-Lite (Search Enabled)...")
        
        # Using the new model and the thinking_level parameter from your docs
        response = client.models.generate_content(
            model="gemini-3.1-flash-lite-preview",
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[{"google_search": {}}],
                thinking_config=types.ThinkingConfig(thinking_level="low") # Low latency for simple tasks # type: ignore
            )
        )
        return (response.text or "").strip()
    
    except Exception as e:
        logger.info(f"[!] AI Analysis failed: {e}")
        # Graceful fallback: If search or Flash-Lite fails, try standard logic without search
        if "429" in str(e):
             logger.info("[!] Quota exceeded. Attempting fallback without search...")
             try:
                 response = client.models.generate_content(
                     model="gemini-3.1-flash-lite-preview",
                     contents="Note: Avoid live search. " + prompt
                 )
                 return "⚠️ *News search unavailable (Quota). Analysis based on stats only.*\n\n" + (response.text or "").strip()
             except Exception as fallback_error:
                logger.info(f"[!] Fallback also failed: {fallback_error}")
                return ""
        return ""

@timer
def send_discord_alert(df, webhook_url):
    if df.empty:
        return
        
    logger.info("\n[*] Formatting and sending Discord alert (Embed Version)...")
    
    qualified_plays = df[df['Edge %'].abs() >= 30.0].copy()
    if qualified_plays.empty:
        return

    qualified_plays['Abs Edge'] = qualified_plays['Edge %'].abs()
    
    # 1. ISOLATE CATEGORIES
    # Get any Points props approved by the ML model
    ml_points_plays = qualified_plays[qualified_plays['Stat'] == "Points"].sort_values(by='Abs Edge', ascending=False).head(3)
    
    # Get the best Combo/Peripheral props (excluding Points)
    combo_stats = ["Rebounds", "Assists", "Pts+Rebs+Asts", "Pts+Rebs", "Pts+Asts", "Rebs+Asts"]
    combo_plays = qualified_plays[qualified_plays['Stat'].isin(combo_stats)].sort_values(by='Abs Edge', ascending=False).head(3)
    
    # Get the best Defensive props (Micro)
    micro_df = qualified_plays[qualified_plays['Stat'].isin(MICRO_STATS)].sort_values(by='Abs Edge', ascending=False).head(3)

    # Combine all plays for AI analysis
    combined_top_plays = pd.concat([ml_points_plays, combo_plays, micro_df])
    
    ai_text = get_ai_analysis(combined_top_plays)

    def format_rows(segment_df):
        msg = ""
        for index, row in segment_df.iterrows():
            emoji = "📈" if row['Play'] == "OVER" else "📉"
            diff_str = f"+{row['Diff']}" if row['Diff'] > 0 else f"{row['Diff']}"
            
            # Format the ML Probability badge if this is a Points prop
            ml_badge = ""
            # Ensure 'ML Prob' exists, is not NaN, AND is not our "-" string
            if 'ML Prob' in row and pd.notna(row['ML Prob']) and row['ML Prob'] != "-":
                ml_prob = float(row['ML Prob'])
                display_prob = ml_prob if row['Play'] == "OVER" else round(100 - ml_prob, 1)
                ml_badge = f" | 🤖 **ML Conf: {display_prob}%**"

            msg += f"**{row['Player']}** ({row['Team']}) | {row['Stat']}\n"
            msg += f"> 🥊 Matchup: {row['Matchup']}\n"
            msg += f"> 🎯 Line: **{row['PP Line']}** | Play: {emoji} **{row['Play']}**\n"
            msg += f"> 📊 15g Med: **{row['15g Median']}** | 5g Med: **{row['5g Median']}**\n"
            msg += f"> ⚖️ Edge: **{row['Edge %']}%**{ml_badge}\n\n"
        return msg

    embeds = []
    if ai_text:
        embeds.append({"title": "🤖 AI Situational Analysis", "description": ai_text, "color": 3447003})

    # AI-VALIDATED POINTS (The "Brain" Picks) - Gets top priority slot
    if not ml_points_plays.empty:
        embeds.append({"title": "🤖 AI-VALIDATED POINTS (High Rest/Trend)", "description": format_rows(ml_points_plays), "color": 65280})

    # HIGH-ALPHA COMBOS (The "Math" Picks)
    if not combo_plays.empty:
        embeds.append({"title": "📊 TOP CORE COMBOS", "description": format_rows(combo_plays), "color": 15105570})

    # DEFENSIVE PLAYS (Micro)
    if not micro_df.empty:
        embeds.append({"title": "🛡️ TOP MICRO PLAYS", "description": format_rows(micro_df), "color": 3066993})

    payload = {
        "content": "🚨 **SHARP EDGE ALERT** 🚨",
        "embeds": embeds,
        "username": "SharpEdge Bot"
    }
    
    # Check if the CSV exists to attach it
    csv_path = "data/daily_picks.csv"
    if os.path.exists(csv_path):
        with open(csv_path, "rb") as f:
            files = {
                "file": ("daily_picks.csv", f, "text/csv")
            }
            # When sending files, the embed must be sent as a payload_json string
            requests.post(webhook_url, data={"payload_json": json.dumps(payload)}, files=files)
    else:
        # Fallback if no CSV
        requests.post(webhook_url, data=json.dumps(payload), headers={'Content-Type': 'application/json'})
    logger.info("[+] Discord alert sent successfully!")

def send_grading_report(summary, csv_path, webhook_url):
    """Sends a clean summary of graded bets to Discord and attaches the full CSV."""
    if not webhook_url:
        logger.error("[!] No Discord webhook URL found.")
        return

    logger.info("[*] Formatting and sending grading report...")
    
    wr_emoji = "🔥" if summary['win_rate'] >= 53.0 else "⚠️"
    
    embed = {
        "title": "⚖️ DAILY GRADING SUMMARY",
        "color": 0xFFD700, # Gold color
        "description": "The Live-Pivot grader has completed the midnight batch.\nFull results are attached below.",
        "fields": [
            {
                "name": "📊 Daily Record",
                "value": f"✅ Wins: **{summary['wins']}**\n❌ Losses: **{summary['losses']}**\n🟰 Pushes: **{summary['pushes']}**\n🚫 Voids: **{summary['voids']}**",
                "inline": True
            },
            {
                "name": "📈 Performance",
                "value": f"{wr_emoji} Win Rate: **{summary['win_rate']}%**\n⏳ Pending: **{summary['skipped']}** games",
                "inline": True
            }
        ]
    }
    
    payload = {"embeds": [embed]}
    
    try:
        # Check if we have a CSV to attach
        if csv_path and os.path.exists(csv_path):
            with open(csv_path, "rb") as f:
                filename = os.path.basename(csv_path)
                files = {"file": (filename, f, "text/csv")}
                response = requests.post(webhook_url, data={"payload_json": json.dumps(payload)}, files=files)
        else:
            response = requests.post(webhook_url, json=payload)
            
        if response.status_code in [200, 204]:
            logger.info("[+] Grading report sent successfully!")
        else:
            logger.error(f"[!] Failed to send grading report. Status Code: {response.status_code}")
    except Exception as e:
        logger.error(f"[!] Exception during Discord delivery: {e}")
