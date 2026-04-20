# src/notifier.py
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
    
    core_df = qualified_plays[qualified_plays['Stat'].isin(CORE_STATS)].sort_values(by='Abs Edge', ascending=False).head(3)
    micro_df = qualified_plays[qualified_plays['Stat'].isin(MICRO_STATS)].sort_values(by='Abs Edge', ascending=False).head(3)

    combined_top_plays = pd.concat([core_df, micro_df])
    
    ai_text = get_ai_analysis(combined_top_plays)

    def format_rows(segment_df):
        msg = ""
        for index, row in segment_df.iterrows():
            emoji = "📈" if row['Play'] == "OVER" else "📉"
            diff_str = f"+{row['Diff']}" if row['Diff'] > 0 else f"{row['Diff']}"
            
            # Format the ML Probability badge if this is a Points prop
            ml_badge = ""
            if 'ML Prob' in row and pd.notna(row['ML Prob']):
                # If it's an OVER play, show probability of OVER. If UNDER, show probability of UNDER (100 - over)
                display_prob = row['ML Prob'] if row['Play'] == "OVER" else round(100 - row['ML Prob'], 1)
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

    if not core_df.empty:
        embeds.append({"title": "🔥 Top Core Plays", "description": format_rows(core_df), "color": 15105570})

    if not micro_df.empty:
        embeds.append({"title": "🛡️ Top Micro Plays", "description": format_rows(micro_df), "color": 3066993})

    payload = {
        "content": "🚨 **SHARP EDGE ALERT** 🚨",
        "embeds": embeds,
        "username": "SharpEdge Bot"
    }
    
    requests.post(webhook_url, data=json.dumps(payload), headers={'Content-Type': 'application/json'})
    logger.info("[+] Discord alert sent successfully!")

def send_grading_report(graded_results, summary, webhook_url):
    """
    Sends a grading report to Discord with the results of graded bets.
    """
    if not graded_results:
        return

    logger.info("\n[*] Formatting and sending grading report...")

    # Build the message
    message = "📊 **GRADING REPORT** 📊\n\n"
    message += f"**Session Summary:** {summary['wins']}W - {summary['losses']}L - {summary['pushes']}P ({summary['win_rate']:.1f}%)\n\n"

    for result in graded_results:
        emoji = (
            "✅"
            if result["status"] == "WIN"
            else "❌"
            if result["status"] == "LOSS"
            else "➖"
        )
        message += f"{emoji} **{result['player']}** | {result['stat']} {result['line']} {result['play']}\n"
        message += f"> Actual: {result['actual']} | Result: **{result['status']}**\n\n"

    payload = {"content": message, "username": "SharpEdge Bot"}

    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
        )
        if response.status_code == 204:
            logger.info("[+] Grading report sent successfully!")
        else:
            logger.info(f"[!] Failed to send grading report. Status: {response.status_code}")
    except Exception as e:
        logger.info(f"[!] Grading report webhook error: {e}")
