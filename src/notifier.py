# src/notifier.py
import requests
import json

# Define our two buckets
CORE_STATS = ["Points", "Rebounds", "Assists", "Pts+Rebs+Asts", "Pts+Rebs", "Pts+Asts", "Rebs+Asts"]
MICRO_STATS = ["3-PT Made", "Blocked Shots", "Steals", "Turnovers", "Blks+Stls"]

def send_discord_alert(df, webhook_url):
    if df.empty:
        return
        
    print("\n[*] Formatting and sending Discord alert...")
    
    # 1. Filter for plays that meet the 30% minimum threshold
    qualified_plays = df[df['Edge %'].abs() >= 30.0].copy()
    
    if qualified_plays.empty:
        print("[-] No premium plays (>30% edge) found. Skipping alert.")
        return

    qualified_plays['Abs Edge'] = qualified_plays['Edge %'].abs()
    
    # 2. SEGMENT THE BOARD
    core_df = qualified_plays[qualified_plays['Stat'].isin(CORE_STATS)].sort_values(by='Abs Edge', ascending=False).head(3)
    micro_df = qualified_plays[qualified_plays['Stat'].isin(MICRO_STATS)].sort_values(by='Abs Edge', ascending=False).head(3)

    # Format the message
    message = "🚨 **SHARP EDGE ALERT** 🚨\n"
    
    def format_rows(segment_df):
        msg = ""
        for index, row in segment_df.iterrows():
            emoji = "📈" if row['Play'] == "OVER" else "📉"
            diff_str = f"+{row['Diff']}" if row['Diff'] > 0 else f"{row['Diff']}"
            msg += f"**{row['Player']}** ({row['Team']}) | {row['Stat']}\n"
            msg += f"> 🥊 Matchup: {row['Matchup']}\n"
            msg += f"> 🎯 Line: **{row['PP Line']}** | Play: {emoji} **{row['Play']}**\n"
            msg += f"> 📊 15g Med: **{row['15g Median']}** | 5g Med: **{row['5g Median']}** | Avg: {row['15g Avg']}\n"
            msg += f"> ⚖️ Diff: {diff_str} | Edge: **{row['Edge %']}%**\n\n"
        return msg

    if not core_df.empty:
        message += "\n🔥 **TOP CORE PLAYS (PTS/REB/AST)** 🔥\n"
        message += format_rows(core_df)
        
    if not micro_df.empty:
        message += "🛡️ **TOP MICRO PLAYS (DEF/3PT)** 🛡️\n"
        message += format_rows(micro_df)

    payload = {
        "content": message,
        "username": "SharpEdge Bot"
    }
    
    try:
        response = requests.post(webhook_url, data=json.dumps(payload), headers={'Content-Type': 'application/json'})
        if response.status_code == 204:
            print("[+] Discord alert sent successfully!")
        else:
            print(f"[!] Failed to send Discord alert. Status: {response.status_code}")
    except Exception as e:
        print(f"[!] Discord Webhook error: {e}")


def send_grading_report(graded_bets, summary, webhook_url):
    """
    Formats the daily grading results and pushes a summary to Discord.
    """
    if not graded_bets:
        print("[-] No new bets were graded today. Skipping Discord report.")
        return

    print("\n[*] Formatting and sending Grading Report to Discord...")
    
    message = "📊 **DAILY GRADING REPORT** 📊\n\n"
    
    # 1. List all the graded bets
    for bet in graded_bets:
        if bet['status'] == 'WIN':
            emoji = "✅"
        elif bet['status'] == 'LOSS':
            emoji = "❌"
        elif bet['status'] == 'PUSH':
            emoji = "🔄"
        else:
            emoji = "⚠️" # VOID
            
        message += f"{emoji} **{bet['player']}** | {bet['stat']} | {bet['play']} {bet['line']} ➔ Actual: **{bet['actual']}** ({bet['status']})\n"

    # 2. Add the summary block
    message += "\n=== **SUMMARY** ===\n"
    message += f"Wins: {summary['wins']} | Losses: {summary['losses']} | Pushes: {summary['pushes']} | Voids: {summary['voids']}\n"
    
    if summary['wins'] + summary['losses'] > 0:
        message += f"**Win Rate:** {summary['win_rate']:.1f}%\n"

    payload = {
        "content": message,
        "username": "SharpEdge Grader"
    }
    
    try:
        response = requests.post(webhook_url, data=json.dumps(payload), headers={'Content-Type': 'application/json'})
        if response.status_code == 204:
            print("[+] Grading report sent to Discord successfully!")
        else:
            print(f"[!] Failed to send Discord grading report. Status: {response.status_code}")
    except Exception as e:
        print(f"[!] Discord Webhook error: {e}")