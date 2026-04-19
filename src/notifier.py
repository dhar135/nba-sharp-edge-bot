# src/notifier.py
import requests
import json

def send_discord_alert(df, webhook_url):
    """
    Formats the top edges and pushes a notification to Discord.
    """
    if df.empty:
        return
        
    print("\n[*] Formatting and sending Discord alert...")
    
    # 1. Filter for plays that meet the 30% minimum threshold (both positive and negative)
    qualified_plays = df[df['Edge %'].abs() >= 30.0].copy()
    
    if qualified_plays.empty:
        print("[-] No premium plays (>30% edge) found. Skipping alert.")
        return

    # 2. Sort by the highest absolute edge percentage to rank Overs and Unders equally
    qualified_plays['Abs Edge'] = qualified_plays['Edge %'].abs()
    premium_plays = qualified_plays.sort_values(by='Abs Edge', ascending=False).head(5)

    # Format the message
    message = "🚨 **SHARP EDGE ALERT** 🚨\n\n"
    
    for index, row in premium_plays.iterrows():
        emoji = "📈" if row['Play'] == "OVER" else "📉"
        message += f"**{row['Player']}** | {row['Stat']}\n"
        message += f"> Line: {row['PP Line']} | True Median: {row['15g Median']}\n"
        message += f"> Edge: **{row['Edge %']}%** | Play: {emoji} **{row['Play']}**\n\n"

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