# src/extractors/nba_extractors.py
import pandas as pd
import time
from nba_api.stats.endpoints import leaguedashplayerstats, leaguedashteamstats, leaguedashptstats
from utils.utils import logger, timer


@timer
def get_advanced_player_baselines(season="2023-24", last_n_games=0):
    """
    Fetches Advanced metrics (USG%, TS%, AST%, TRB%, PACE) for all players.
    Using 'last_n_games' allows us to detect recent form/rotation changes.
    """
    logger.info(f"[*] Fetching Advanced Player Baselines (Last {last_n_games} games)...")
    try:
        stats = leaguedashplayerstats.LeagueDashPlayerStats(
            measure_type_detailed_defense='Advanced',
            per_mode_detailed='PerGame',
            season=season,
            last_n_games=last_n_games
        )
        df = stats.get_data_frames()[0]
        # Keep only the deterministic pillars we care about
        cols_to_keep = ['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ABBREVIATION', 'MIN', 'USG_PCT', 'TS_PCT', 'AST_PCT', 'REB_PCT', 'PACE']
        return df[cols_to_keep]
    except Exception as e:
        logger.error(f"[!] Failed to fetch advanced player stats: {e}")
        return pd.DataFrame()


@timer
def get_team_pace_and_defense(season="2023-24"):
    """
    Fetches Advanced Team metrics to determine opponent Pace and Defensive Rating.
    Crucial for calculating expected possessions in a specific matchup.
    """
    logger.info("[*] Fetching Team Pace and Defensive Ratings...")
    try:
        stats = leaguedashteamstats.LeagueDashTeamStats(
            measure_type_detailed_defense='Advanced',
            per_mode_detailed='PerGame',
            season=season
        )
        df = stats.get_data_frames()[0]
        cols_to_keep = ['TEAM_ID', 'TEAM_NAME', 'PACE', 'DEF_RATING']
        return df[cols_to_keep]
    except Exception as e:
        logger.error(f"[!] Failed to fetch team pace data: {e}")
        return pd.DataFrame()


@timer
def get_tracking_data(season="2023-24"):
    """
    Fetches Spatiotemporal Player Tracking Data.
    We need 'Touches' to project usage accurately, and 'Potential Assists' to strip out teammate shooting variance.
    """
    logger.info("[*] Fetching Player Tracking Data (Touches & Pot. Assists)...")
    try:
        # Fetch Passing Tracking Data (Potential Assists)
        passing_stats = leaguedashptstats.LeagueDashPtStats(
            pt_measure_type='Passing',
            season=season,
            per_mode_simple='PerGame'
        ).get_data_frames()[0]
        
        time.sleep(0.6)  # Be polite to nba_api
        
        # Fetch Possession Tracking Data (Touches)
        touch_stats = leaguedashptstats.LeagueDashPtStats(
            pt_measure_type='Possessions',
            season=season,
            per_mode_simple='PerGame'
        ).get_data_frames()[0]
        
        # Merge the tracking datasets on Player ID
        passing_clean = passing_stats[['PLAYER_ID', 'POTENTIAL_AST']]
        touches_clean = touch_stats[['PLAYER_ID', 'TOUCHES', 'TIME_OF_POSS']]
        
        tracking_df = pd.merge(touches_clean, passing_clean, on='PLAYER_ID', how='inner')
        return tracking_df
        
    except Exception as e:
        logger.error(f"[!] Failed to fetch tracking data: {e}")
        return pd.DataFrame()
