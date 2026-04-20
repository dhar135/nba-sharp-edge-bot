# src/nba_fetcher.py
import pandas as pd
from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog
import time
from nba_api.stats.endpoints import leaguegamelog
from utils import logger, timer


@timer
def get_player_gamelog(player_name, season='2025-26'):
    """
    Fetches the game log for a specific player for the given season.
    """
    logger.info(f"[*] Fetching NBA ID for: {player_name}")
    
    # 1. Find the player dictionary
    active_players = players.get_players()
    player_dict = [p for p in active_players if p['full_name'].lower() == player_name.lower()]
    
    if not player_dict:
        logger.info(f"[!] Error: Could not find player {player_name}")
        return None
        
    player_id = player_dict[0]['id']
    
    # 2. Fetch the game log
    logger.info(f"[*] Fetching game logs for ID: {player_id}")
    try:
        # Note: For playoffs, you'd eventually pass SeasonType='Playoffs'
        gamelog = playergamelog.PlayerGameLog(player_id=player_id, season=season)
        df = gamelog.get_data_frames()[0]
        
        # Be polite to the API
        time.sleep(0.6) 
        
        return df
    except Exception as e:
        logger.info(f"[!] Failed to fetch data from NBA API: {e}")
        return None
    

@timer
def get_league_gamelog(season="2025-26"):
    """
    The God-Call. Fetches every single box score for every player in the NBA 
    for the current season in one massive DataFrame.
    """
    logger.info(f"\n[*] Fetching God-Level League Game Log for {season}...")
    try:
        # player_or_team_abbreviation='P' tells it to get player logs instead of team logs
        log = leaguegamelog.LeagueGameLog(player_or_team_abbreviation='P', season=season)
        df = log.get_data_frames()[0]
        
        # Ensure dates are properly formatted so we can sort chronologically
        df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
        
        # Sort the entire league by Player Name, then by Date (Newest to Oldest)
        df = df.sort_values(by=['PLAYER_NAME', 'GAME_DATE'], ascending=[True, False])
        
        # --- VECTORIZED MATH ---
        # Pre-calculate all combo stats for all players instantly
        df['PRA'] = df['PTS'] + df['REB'] + df['AST']
        df['PR'] = df['PTS'] + df['REB']
        df['PA'] = df['PTS'] + df['AST']
        df['RA'] = df['REB'] + df['AST']
        df['BS'] = df['BLK'] + df['STL']
        
        logger.info(f"[+] Successfully loaded {len(df)} box scores into memory.")
        return df
        
    except Exception as e:
        logger.info(f"[!] Failed to fetch League Game Log: {e}")
        return None