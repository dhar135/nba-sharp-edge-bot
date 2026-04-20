# src/main.py
import os
import pandas as pd
import cloudscraper
from engine import calculate_all_edges
from notifier import send_discord_alert
from dotenv import load_dotenv
from db import init_db, log_predictions
from grader import grade_pending_bets
from constants import SUPPORTED_STATS

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
    
    # 1. Build lookup dictionaries for players and leagues
    players = {}
    leagues = {}
    
    for item in json_data.get('included', []):
        if item['type'] == 'new_player':
            # NEW: Store both name and team
            players[item['id']] = {
                'name': item['attributes'].get('display_name') or item['attributes'].get('name'),
                'team': item['attributes'].get('team', 'UNK')
            }
        elif item['type'] == 'league':
            leagues[item['id']] = item['attributes']['name']

    nba_projections = []
    
    # 2. Iterate through the actual lines
    for proj in json_data.get('data', []):
        if proj['type'] != 'projection':
            continue
            
        attrs = proj['attributes']
        rels = proj['relationships']
        
        league_id = rels.get('league', {}).get('data', {}).get('id')
        player_id = rels.get('new_player', {}).get('data', {}).get('id')
        
        league_name = leagues.get(league_id, "Unknown")
        
        # NEW: Extract name, team, and matchup (description)
        player_info = players.get(player_id, {})
        player_name = player_info.get('name', 'Unknown')
        player_team = player_info.get('team', 'UNK')
        matchup = attrs.get('description', 'Unknown')
        
        if league_name != "NBA":
            continue
            
        stat_type = attrs.get('stat_type')
        line_score = attrs.get('line_score')
        odds_type = attrs.get('odds_type')
        start_time = attrs.get('start_time')
        
        if start_time:
            dt = pd.to_datetime(start_time)
            game_date = dt.tz_convert('US/Eastern').strftime('%Y-%m-%d')
        else:
            game_date = None

        if odds_type != 'standard':
            continue
        
        if stat_type in SUPPORTED_STATS:
            nba_projections.append({
                "Player": player_name,
                "Team": player_team,
                "Matchup": matchup,
                "Stat": stat_type,
                "Line": line_score,
                "Game Date": game_date
            })
            
    df = pd.DataFrame(nba_projections)
    return df

if __name__ == "__main__":
    DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")
    
    # 1. Initialize DB
    init_db()
    
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
            
            # Send Discord Alert
            if DISCORD_WEBHOOK:
                send_discord_alert(edges_df, DISCORD_WEBHOOK)
            else:
                print("[!] Discord alert skipped. No webhook URL found.")
                
            # 2. Log to Database
            log_predictions(edges_df)
            
            # 3. Grade any pending bets
            grade_pending_bets()