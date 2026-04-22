# src/services/notifier.py
"""
Side-effect service: Discord notifications and AI-powered situational analysis
Handles all outbound alerts and reporting
"""
import requests
import json
import os
import pandas as pd
from google import genai
from google.genai import types
from utils.utils import logger, timer

CORE_STATS = ["Points", "Rebounds", "Assists", "Pts+Rebs+Asts", "Pts+Rebs", "Pts+Asts", "Rebs+Asts"]
MICRO_STATS = ["3-PT Made", "Blocked Shots", "Steals", "Turnovers", "Blks+Stls"]

@timer
def get_ai_analysis(plays_df):
    """Feeds top plays to Gemini 3.1 Flash-Lite for live news-aware insights."""
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
        
        response = client.models.generate_content(
            model="gemini-3.1-flash-lite-preview",
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[{"google_search": {}}],
                thinking_config=types.ThinkingConfig(thinking_level="low")
            )
        )
        return (response.text or "").strip()
    
    except Exception as e:
        logger.info(f"[!] AI Analysis failed: {e}")
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
    ml_points_plays = qualified_plays[qualified_plays['Stat'] == "Points"].sort_values(by='Abs Edge', ascending=False).head(3)
    combo_stats = ["Rebounds", "Assists", "Pts+Rebs+Asts", "Pts+Rebs", "Pts+Asts", "Rebs+Asts"]
    combo_plays = qualified_plays[qualified_plays['Stat'].isin(combo_stats)].sort_values(by='Abs Edge', ascending=False).head(3)
    micro_df = qualified_plays[qualified_plays['Stat'].isin(MICRO_STATS)].sort_values(by='Abs Edge', ascending=False).head(3)

    combined_top_plays = pd.concat([ml_points_plays, combo_plays, micro_df])
    ai_text = get_ai_analysis(combined_top_plays)

    def format_rows(segment_df):
        msg = ""
        for index, row in segment_df.iterrows():
            emoji = "📈" if row['Play'] == "OVER" else "📉"
            
            ml_badge = ""
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

    if not ml_points_plays.empty:
        embeds.append({"title": "🤖 AI-VALIDATED POINTS (High Rest/Trend)", "description": format_rows(ml_points_plays), "color": 65280})

    if not combo_plays.empty:
        embeds.append({"title": "📊 TOP CORE COMBOS", "description": format_rows(combo_plays), "color": 15105570})

    if not micro_df.empty:
        embeds.append({"title": "🛡️ TOP MICRO PLAYS", "description": format_rows(micro_df), "color": 3066993})

    payload = {
        "content": "🚨 **SHARP EDGE ALERT** 🚨",
        "embeds": embeds,
        "username": "SharpEdge Bot"
    }
    
    csv_path = "data/daily_picks.csv"
    if os.path.exists(csv_path):
        with open(csv_path, "rb") as f:
            files = {"file": ("daily_picks.csv", f, "text/csv")}
            requests.post(webhook_url, data={"payload_json": json.dumps(payload)}, files=files)
    else:
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
        "color": 0xFFD700,
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
