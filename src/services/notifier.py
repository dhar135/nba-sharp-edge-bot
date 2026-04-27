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
def send_discord_alert(plays_df, webhook_url):
    """
    Pushes a batched V2.0 alert to Discord. 
    Shows AI analysis, Top 5 plays in the embed, and attaches the full CSV.
    """
    if plays_df.empty:
        return
        
    logger.info(f"[*] Formatting {len(plays_df)} plays and generating CSV...")
    
    # 1. Save ALL plays to CSV
    os.makedirs("data", exist_ok=True)
    csv_path = "data/v2_active_plays.csv"
    plays_df.to_csv(csv_path, index=False)

    # 2. Isolate Top Plays for the Embed (Prevents Discord text limits)
    # V2.1: All plays reaching this point are already cleared (vetoed plays excluded upstream)
    # Sort by Confidence score instead of raw edge (confidence accounts for stat reliability)
    sort_col = 'Confidence' if 'Confidence' in plays_df.columns else 'EV Edge'
    cleared_plays = plays_df.sort_values(by=sort_col, ascending=False)
    display_plays = cleared_plays.head(5)
    
    # 3. Get Gemini AI Situational Analysis
    ai_text = get_ai_analysis(display_plays)

    # 4. Build Embeds
    embeds = []
    if ai_text:
        embeds.append({
            "title": "🤖 Gemini Situational Analysis", 
            "description": ai_text, 
            "color": 3447003
        })

    # Format the display rows
    msg = ""
    for _, row in display_plays.iterrows():
        tier_str = row.get('Tier', '✅')
        confidence = row.get('Confidence', 0)
        msg += f"**{row['Player']}** ({row['Team']}) | {row['Stat']}\n"
        msg += f"> 🎯 Line: **{row['PP Line']}** | Play: **{row['Play']}**\n"
        msg += f"> 📊 V2.1 Proj: **{row['V2 Proj']:.2f}** | Prob: **{row['Poisson Prob']:.1f}%**\n"
        msg += f"> ⚖️ Edge: **{row['EV Edge']:.2f}%** | Confidence: **{confidence:.0f}** | {tier_str}\n\n"

    embeds.append({
        "title": f"⚡ TOP V2.1 PLAYS (Showing {len(display_plays)} of {len(plays_df)})",
        "description": msg,
        "color": 65280,
        "footer": {"text": "NBA Sharp Edge V2.1 • NegBin + Strategy Filter • Full list in attached CSV"}
    })

    payload = {
        "content": "🚨 **SHARP EDGE V2.1 ALERT** 🚨",
        "embeds": embeds,
        "username": "SharpEdge Bot"
    }
    
    # 5. Send Single Payload with CSV
    try:
        with open(csv_path, "rb") as f:
            files = {"file": ("v2_active_plays.csv", f, "text/csv")}
            # payload_json is required by Discord API when sending files alongside embeds
            response = requests.post(webhook_url, data={"payload_json": json.dumps(payload)}, files=files)
            
        if response.status_code in [200, 204]:
            logger.info("[+] Discord batched alert with CSV sent successfully!")
        else:
            logger.error(f"[!] Discord Webhook returned {response.status_code}")
    except Exception as e:
        logger.error(f"[!] Failed to send Discord alert: {e}")

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
