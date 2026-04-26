# src/extractors/pp_extractors.py
import pandas as pd
import cloudscraper
from utils.constants import SUPPORTED_STATS
from utils.utils import logger, timer


@timer
def fetch_live_board():
    """
    Stateless function to fetch and parse the live PrizePicks board into a DataFrame.
    """
    logger.info("[*] Fetching Live PrizePicks Board...")
    url = "https://api.prizepicks.com/projections"
    scraper = cloudscraper.create_scraper()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }
    
    try:
        response = scraper.get(url, headers=headers)
        if response.status_code != 200:
            logger.error(f"[!] PrizePicks API Error: {response.status_code}")
            return pd.DataFrame()
            
        json_data = response.json()
        return _parse_board_json(json_data)
        
    except Exception as e:
        logger.error(f"[!] Scraper failed: {e}")
        return pd.DataFrame()


def _parse_board_json(json_data):
    """
    Internal helper to parse PrizePicks JSON payload into a clean DataFrame.
    """
    players = {}
    leagues = {}
    
    # Build Lookups
    for item in json_data.get('included', []):
        if item['type'] == 'new_player':
            players[item['id']] = {
                'name': item['attributes'].get('display_name') or item['attributes'].get('name'),
                'team': item['attributes'].get('team', 'UNK')
            }
        elif item['type'] == 'league':
            leagues[item['id']] = item['attributes']['name']

    nba_projections = []
    
    # Parse Lines
    for proj in json_data.get('data', []):
        if proj['type'] != 'projection':
            continue
            
        attrs = proj['attributes']
        rels = proj['relationships']
        league_id = rels.get('league', {}).get('data', {}).get('id')
        player_id = rels.get('new_player', {}).get('data', {}).get('id')
        
        if leagues.get(league_id) != "NBA" or attrs.get('odds_type') != 'standard':
            continue
            
        stat_type = attrs.get('stat_type')
        if stat_type in SUPPORTED_STATS:
            player_info = players.get(player_id, {})
            
            # Extract game date if available
            start_time = attrs.get('start_time')
            game_date = None
            if start_time:
                dt = pd.to_datetime(start_time)
                game_date = dt.tz_convert('US/Eastern').strftime('%Y-%m-%d')
            
            nba_projections.append({
                "Player": player_info.get('name', 'Unknown'),
                "Team": player_info.get('team', 'UNK'),
                "Matchup": attrs.get('description', 'Unknown'),
                "Stat": stat_type,
                "Line": attrs.get('line_score'),
                "Game Date": game_date
            })
            
    return pd.DataFrame(nba_projections)
