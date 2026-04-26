# src/extractors/nba_extractors.py
import pandas as pd
import time
from nba_api.stats.endpoints import leaguedashplayerstats, leaguedashteamstats, leaguedashptstats
from utils.utils import logger, timer
from nba_api.stats.static import teams # Import static teams dictionary


@timer
def get_advanced_player_baselines(season="2025-26", last_n_games=15, season_type="Regular Season"):
    logger.info(f"[*] Fetching Advanced & Base Player Stats (Last {last_n_games} games, SeasonType={season_type})...")
    try:
        # 1. Fetch Advanced Stats (Pace, USG%, TS%)
        adv_stats = leaguedashplayerstats.LeagueDashPlayerStats(
            measure_type_detailed_defense='Advanced',
            per_mode_detailed='PerGame',
            season=season,
            season_type_all_star=season_type,
            last_n_games=last_n_games
        ).get_data_frames()[0]
        
        time.sleep(0.6) 
        
        # 2. Fetch Base Stats (PTS, AST, REB)
        base_stats = leaguedashplayerstats.LeagueDashPlayerStats(
            measure_type_detailed_defense='Base',
            per_mode_detailed='PerGame',
            season=season,
            season_type_all_star=season_type,
            last_n_games=last_n_games
        ).get_data_frames()[0]
        
        # Merge them together
        adv_clean = adv_stats[['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ABBREVIATION', 'MIN', 'USG_PCT', 'TS_PCT', 'AST_PCT', 'REB_PCT', 'PACE']]
        base_clean = base_stats[['PLAYER_ID', 'PTS', 'AST', 'REB']]
        
        merged_df = pd.merge(adv_clean, base_clean, on='PLAYER_ID', how='inner')
        return merged_df
        
    except Exception as e:
        logger.error(f"[!] Failed to fetch player baselines: {e}")
        return pd.DataFrame()


@timer
def get_team_pace_and_defense(season="2025-26"):
    logger.info("[*] Fetching Team Pace and Defensive Ratings...")
    try:
        stats = leaguedashteamstats.LeagueDashTeamStats(
            measure_type_detailed_defense='Advanced',
            per_mode_detailed='PerGame',
            season=season
        )
        df = stats.get_data_frames()[0]
        
        # Build a mapping dictionary: {1610612747: 'LAL', ...}
        nba_teams = teams.get_teams()
        team_mapping = {team['id']: team['abbreviation'] for team in nba_teams}
        
        # Map the abbreviations natively into the Extractor DataFrame
        df['TEAM_ABBREVIATION'] = df['TEAM_ID'].map(team_mapping)
        
        cols_to_keep = ['TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'PACE', 'DEF_RATING']
        return df[cols_to_keep]
    except Exception as e:
        logger.error(f"[!] Failed to fetch team pace data: {e}")
        return pd.DataFrame()


@timer
def get_tracking_data(season="2025-26"):
    logger.info("[*] Fetching Player Tracking Data (Touches & Pot. Assists)...")
    try:
        # Add player_or_team='Player'
        passing_stats = leaguedashptstats.LeagueDashPtStats(
            pt_measure_type='Passing',
            player_or_team='Player', 
            season=season,
            per_mode_simple='PerGame'
        ).get_data_frames()[0]
        
        if passing_stats.empty:
            logger.warning("[-] Passing tracking data returned empty.")
            return pd.DataFrame()
            
        time.sleep(0.6) 
        
        # Add player_or_team='Player'
        touch_stats = leaguedashptstats.LeagueDashPtStats(
            pt_measure_type='Possessions',
            player_or_team='Player', 
            season=season,
            per_mode_simple='PerGame'
        ).get_data_frames()[0]
        
        if touch_stats.empty:
            logger.warning("[-] Touch tracking data returned empty.")
            return pd.DataFrame()
            
        passing_clean = passing_stats[['PLAYER_ID', 'POTENTIAL_AST']]
        touches_clean = touch_stats[['PLAYER_ID', 'TOUCHES', 'TIME_OF_POSS']]
        
        return pd.merge(touches_clean, passing_clean, on='PLAYER_ID', how='inner')
        
    except Exception as e:
        logger.error(f"[!] Failed to fetch tracking data: {e}")
        return pd.DataFrame()
