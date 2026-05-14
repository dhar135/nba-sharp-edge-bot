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

# Project root for resolving paths
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_NOTES_FILE = os.path.join(_PROJECT_ROOT, "notes.txt")
_DATA_DIR = os.path.join(_PROJECT_ROOT, "data")

CORE_STATS = ["Points", "Rebounds", "Assists", "Pts+Rebs+Asts", "Pts+Rebs", "Pts+Asts", "Rebs+Asts"]
MICRO_STATS = ["3-PT Made", "Blocked Shots", "Steals", "Turnovers", "Blks+Stls"]


def _read_and_clear_notes():
    """
    Reads the notes.txt file from the project root and returns its contents.
    Clears the file after reading so notes don't repeat on the next alert cycle.

    Usage: before running the pipeline, drop anything into notes.txt:
        echo "Jokic listed questionable - knee. Tatum playing through ankle." > notes.txt
    """
    if not os.path.exists(_NOTES_FILE):
        return ""
    try:
        with open(_NOTES_FILE, "r") as f:
            content = f.read().strip()
        if content:
            # Clear after reading so it doesn't repeat next cycle
            with open(_NOTES_FILE, "w") as f:
                f.write("")
            logger.info(f"[*] Loaded analyst notes ({len(content)} chars). Clearing for next cycle.")
        return content
    except Exception as e:
        logger.warning(f"[!] Could not read notes.txt: {e}")
        return ""


@timer
def send_discord_alert(plays_df, webhook_url):
    """
    Pushes a batched V2.1 alert to Discord.
    - Groups top plays by game date so you can easily build same-day parlays
    - Shows analyst notes from notes.txt if present
    - Attaches full CSV of all plays
    """
    if plays_df.empty:
        return

    logger.info(f"[*] Formatting {len(plays_df)} plays and generating CSV...")

    # 1. Save ALL plays to CSV
    os.makedirs(_DATA_DIR, exist_ok=True)
    csv_path = os.path.join(_DATA_DIR, "v2_active_plays.csv")
    plays_df.to_csv(csv_path, index=False)

    # 2. Sort by confidence, show top 6
    sort_col = 'Confidence' if 'Confidence' in plays_df.columns else 'EV Edge'
    display_plays = plays_df.sort_values(by=sort_col, ascending=False).head(6)

    # 3. Read analyst notes (from notes.txt — written by you before running)
    analyst_notes = _read_and_clear_notes()

    # 4. Build Embeds
    embeds = []

    # Analyst notes embed (only if notes.txt had content)
    if analyst_notes:
        embeds.append({
            "title": "📝 Analyst Notes",
            "description": analyst_notes,
            "color": 0xF4C430  # Gold
        })

    # 5. Group plays by game date for easy parlay building
    has_dates = 'Game Date' in display_plays.columns and display_plays['Game Date'].notna().any()

    if has_dates:
        grouped = display_plays.groupby('Game Date', sort=True)
        for game_date, group in grouped:
            msg = ""
            for _, row in group.iterrows():
                tier_str = row.get('Tier', '✅')
                confidence = row.get('Confidence', 0)
                msg += f"**{row['Player']}** ({row['Team']}) | {row['Stat']}\n"
                msg += f"> 🎯 Line: **{row['PP Line']}** | Play: **{row['Play']}**\n"
                msg += f"> 📊 Proj: **{row['V2 Proj']:.2f}** | Prob: **{row['Poisson Prob']:.1f}%**\n"
                msg += f"> ⚖️ Edge: **{row['EV Edge']:.2f}%** | Conf: **{confidence:.0f}** | {tier_str}\n\n"

            embeds.append({
                "title": f"📅 {game_date}  —  {len(group)} play(s)",
                "description": msg,
                "color": 0x00FF88
            })
    else:
        # No date grouping — show as flat list
        msg = ""
        for _, row in display_plays.iterrows():
            tier_str = row.get('Tier', '✅')
            confidence = row.get('Confidence', 0)
            game_date = row.get('Game Date', None)
            date_str = f" | 📅 **{game_date}**" if game_date else ""
            msg += f"**{row['Player']}** ({row['Team']}) | {row['Stat']}{date_str}\n"
            msg += f"> 🎯 Line: **{row['PP Line']}** | Play: **{row['Play']}**\n"
            msg += f"> 📊 Proj: **{row['V2 Proj']:.2f}** | Prob: **{row['Poisson Prob']:.1f}%**\n"
            msg += f"> ⚖️ Edge: **{row['EV Edge']:.2f}%** | Conf: **{confidence:.0f}** | {tier_str}\n\n"

        embeds.append({
            "title": f"⚡ TOP V2.1 PLAYS (Showing {len(display_plays)} of {len(plays_df)})",
            "description": msg,
            "color": 0x00FF88,
            "footer": {"text": "NBA Sharp Edge V2.1 • Full list in attached CSV"}
        })

    # Footer embed with summary stats
    total = len(plays_df)
    green = len(plays_df[plays_df.get('Tier', pd.Series()).str.contains('🟢', na=False)]) if 'Tier' in plays_df.columns else 0
    embeds.append({
        "title": "📊 Session Summary",
        "description": f"**{total}** plays cleared all filters | **{green}** 🟢 ELITE/STRONG tier\nFull board attached as CSV.",
        "color": 0x2F3136,
        "footer": {"text": "NBA Sharp Edge V2.1 • NegBin + Strategy Filter + Veto Layer"}
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
