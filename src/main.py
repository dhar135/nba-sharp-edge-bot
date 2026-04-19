# src/main.py
import os
import pandas as pd
import cloudscraper
from engine import calculate_all_edges
from notifier import send_discord_alert
from dotenv import load_dotenv

load_dotenv()

def fetch_prizepicks_board():
    print("[*] Attempting to fetch PrizePicks board via Cloudscraper...")
    url = "https://api.prizepicks.com/projections"
    
    # We create a scraper object that acts exactly like 'requests' but spoofs TLS/Browser signatures
    scraper = cloudscraper.create_scraper() 
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://app.prizepicks.com/",
        "Origin": "https://app.prizepicks.com"
    }
    
    try:
        response = scraper.get(url, headers=headers)
        if response.status_code == 200:
            print("[+] Successfully fetched PrizePicks board.")
            return response.json()
        else:
            print(f"[!] Failed to fetch PrizePicks. Status Code: {response.status_code}")
            return None
    except Exception as e:
        print(f"[!] Request error: {e}")
        return None

def parse_prizepicks_json(json_data):
    """
    Parses the massive JSON payload into a clean Pandas DataFrame containing only NBA props.
    """
    print("[*] Parsing JSON payload...")
    
    # 1. Build lookup dictionaries for players and leagues from the "included" array
    players = {}
    leagues = {}
    
    for item in json_data.get('included', []):
        if item['type'] == 'new_player':
            # Store the player's name using their ID as the key
            players[item['id']] = item['attributes']['display_name'] # Sometimes it's 'name'
        elif item['type'] == 'league':
            # Store the league name using its ID as the key
            leagues[item['id']] = item['attributes']['name']

    # 2. Iterate through the actual lines in the "data" array
    nba_projections = []
    
    for proj in json_data.get('data', []):
        if proj['type'] != 'projection':
            continue
            
        attrs = proj['attributes']
        rels = proj['relationships']
        
        # Safely extract IDs
        league_id = rels.get('league', {}).get('data', {}).get('id')
        player_id = rels.get('new_player', {}).get('data', {}).get('id')
        
        # Look up the actual names using our dictionaries
        league_name = leagues.get(league_id, "Unknown")
        player_name = players.get(player_id, "Unknown")
        
        # 3. FILTER: We only care about the NBA
        if league_name != "NBA":
            continue
            
        stat_type = attrs.get('stat_type')
        line_score = attrs.get('line_score')
        odds_type = attrs.get('odds_type')

        # FILTER: We only want standard lines, no Demons/Goblins
        if odds_type != 'standard':
            continue
        
        # Only grab standard stats to start (Points, Rebounds, Assists, PRA)
        target_stats = ["Points", "Rebounds", "Assists", "Pts+Rebs+Asts"]
        if stat_type in target_stats:
            nba_projections.append({
                "Player": player_name,
                "Stat": stat_type,
                "Line": line_score
            })
            
    # 4. Convert to a clean pandas DataFrame
    df = pd.DataFrame(nba_projections)
    return df

if __name__ == "__main__":
    # Pull the webhook securely from the environment
    DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")
    
    print("=== Sharp Edge MVP Initialization ===\n")
    
    pp_data = fetch_prizepicks_board()
    
    if pp_data:
        clean_board = parse_prizepicks_json(pp_data)
        print(f"\n[+] Extracted {len(clean_board)} STANDARD NBA lines.")
        
        edges_df = calculate_all_edges(clean_board, sample_size=15, edge_threshold=15.0)
        
        print("\n=== THE EDGE REPORT (>15% Discrepancies) ===")
        if edges_df.empty:
            print("No massive edges found on the board right now. Market is tight.")
        else:
            edges_df['Abs Diff'] = edges_df['Diff'].abs()
            edges_df = edges_df.sort_values(by='Abs Diff', ascending=False).drop(columns=['Abs Diff'])
            print(edges_df.to_string(index=False))
            
            # Fire the alert as long as the webhook exists in the .env file
            if DISCORD_WEBHOOK:
                send_discord_alert(edges_df, DISCORD_WEBHOOK)
            else:
                print("[!] Discord alert skipped. No webhook URL found in .env file.")