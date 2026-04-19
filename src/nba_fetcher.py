# src/nba_fetcher.py
import pandas as pd
from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog
import time

def get_player_gamelog(player_name, season='2025-26'):
    """
    Fetches the game log for a specific player for the given season.
    """
    print(f"[*] Fetching NBA ID for: {player_name}")
    
    # 1. Find the player dictionary
    active_players = players.get_players()
    player_dict = [p for p in active_players if p['full_name'].lower() == player_name.lower()]
    
    if not player_dict:
        print(f"[!] Error: Could not find player {player_name}")
        return None
        
    player_id = player_dict[0]['id']
    
    # 2. Fetch the game log
    print(f"[*] Fetching game logs for ID: {player_id}")
    try:
        # Note: For playoffs, you'd eventually pass SeasonType='Playoffs'
        gamelog = playergamelog.PlayerGameLog(player_id=player_id, season=season)
        df = gamelog.get_data_frames()[0]
        
        # Be polite to the API
        time.sleep(0.6) 
        
        return df
    except Exception as e:
        print(f"[!] Failed to fetch data from NBA API: {e}")
        return None