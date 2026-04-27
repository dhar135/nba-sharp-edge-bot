# src/extractors/nba_extractors.py
"""
V2.1 NBA Data Extractors — Stateless data fetching layer

Key changes from V2.0:
  1. Added get_opponent_matchup_multipliers() — re-integrated from V1
  2. Added get_league_gamelog_for_ewma() — fetches game logs for EWMA computation
  3. Improved error handling with structured fallback DataFrames
"""
import pandas as pd
import time
from nba_api.stats.endpoints import (
    leaguedashplayerstats,
    leaguedashteamstats,
    leaguedashptstats,
    leaguegamelog,
)
from nba_api.stats.static import teams
from utils.utils import logger, timer

CURRENT_SEASON = "2025-26"


@timer
def get_advanced_player_baselines(season=CURRENT_SEASON, last_n_games=15, season_type="Regular Season"):
    """Fetch merged advanced + base player stats from NBA API."""
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
        adv_clean = adv_stats[['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ABBREVIATION', 'MIN',
                               'USG_PCT', 'TS_PCT', 'AST_PCT', 'REB_PCT', 'PACE']]
        base_clean = base_stats[['PLAYER_ID', 'PTS', 'AST', 'REB']]

        merged_df = pd.merge(adv_clean, base_clean, on='PLAYER_ID', how='inner')
        return merged_df

    except Exception as e:
        logger.error(f"[!] Failed to fetch player baselines: {e}")
        expected_columns = ['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ABBREVIATION', 'MIN',
                           'USG_PCT', 'TS_PCT', 'AST_PCT', 'REB_PCT', 'PACE',
                           'PTS', 'AST', 'REB']
        return pd.DataFrame(columns=expected_columns)


@timer
def get_team_pace_and_defense(season=CURRENT_SEASON):
    """Fetch team-level pace and defensive rating data."""
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
def get_tracking_data(season=CURRENT_SEASON):
    """Fetch player tracking data (touches and potential assists)."""
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


@timer
def get_opponent_matchup_multipliers(season=CURRENT_SEASON):
    """
    Build a per-team, per-stat defensive difficulty multiplier.
    Re-integrated from V1 engine — this was the single biggest missing feature in V2.0.

    Multiplier > 1.0 → this team allows more of that stat than league average (bad defense)
    Multiplier < 1.0 → this team is stingy on that stat (good defense)

    Returns:
        Dict of shape { "LAL": { "Points": 1.05, "Rebounds": 0.97, ... }, ... }
        or empty dict on failure.
    """
    logger.info(f"[*] Fetching defensive matchup multipliers for {season}...")

    # Maps PrizePicks stat names to opponent stat column names
    OPP_STAT_COLUMN_MAP = {
        "Points":        "OPP_PTS",
        "Rebounds":      "OPP_REB",
        "Assists":       "OPP_AST",
        "Pts+Rebs+Asts": "OPP_PRA",
        "Pts+Rebs":      "OPP_PR",
        "Pts+Asts":      "OPP_PA",
        "Rebs+Asts":     "OPP_RA",
        "3-PT Made":     "OPP_FG3M",
        "Blocked Shots": "OPP_BLK",
        "Steals":        "OPP_STL",
        "Turnovers":     "OPP_TOV",
        "Blks+Stls":     "OPP_BS",
    }

    try:
        stats = leaguedashteamstats.LeagueDashTeamStats(
            measure_type_detailed_defense="Opponent",
            per_mode_detailed="PerGame",
            season=season,
        )
        df = stats.get_data_frames()[0]

        # Attach abbreviations
        team_id_to_abbr = {t["id"]: t["abbreviation"] for t in teams.get_teams()}
        df["TEAM_ABBR"] = df["TEAM_ID"].map(team_id_to_abbr)

        # Combo columns
        df["OPP_PRA"] = df["OPP_PTS"] + df["OPP_REB"] + df["OPP_AST"]
        df["OPP_PR"]  = df["OPP_PTS"] + df["OPP_REB"]
        df["OPP_PA"]  = df["OPP_PTS"] + df["OPP_AST"]
        df["OPP_RA"]  = df["OPP_REB"] + df["OPP_AST"]
        df["OPP_BS"]  = df["OPP_BLK"] + df["OPP_STL"]

        # Build multiplier dict
        multipliers = {}
        for stat_name, col in OPP_STAT_COLUMN_MAP.items():
            if col not in df.columns:
                continue
            league_avg = df[col].mean()
            if league_avg == 0:
                continue
            for _, row in df.iterrows():
                team = row["TEAM_ABBR"]
                if team not in multipliers:
                    multipliers[team] = {}
                multipliers[team][stat_name] = round(row[col] / league_avg, 3)

        logger.info(f"[+] Generated defensive multipliers for {len(multipliers)} teams.")
        return multipliers

    except Exception as e:
        logger.error(f"[!] Failed to fetch defensive stats: {e}")
        return {}


@timer
def get_league_gamelog_for_ewma(season=CURRENT_SEASON, season_type="Regular Season"):
    """
    Fetch the league-wide game log for EWMA baseline computation.
    This pulls every player's box score for the season in one call.

    Returns:
        DataFrame with all player game logs, sorted by player/date,
        with combo columns pre-computed. Returns empty DataFrame on failure.
    """
    logger.info(f"[*] Fetching league-wide game log for EWMA baselines ({season_type})...")
    try:
        log = leaguegamelog.LeagueGameLog(
            player_or_team_abbreviation="P",
            season=season,
            season_type_all_star=season_type,
        )
        df = log.get_data_frames()[0]

        df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
        df = df.sort_values(by=["PLAYER_NAME", "GAME_DATE"], ascending=[True, False])

        # Vectorised combo stats
        df["PRA"] = df["PTS"] + df["REB"] + df["AST"]
        df["PR"]  = df["PTS"] + df["REB"]
        df["PA"]  = df["PTS"] + df["AST"]
        df["RA"]  = df["REB"] + df["AST"]
        df["BS"]  = df["BLK"] + df["STL"]

        logger.info(f"[+] Loaded {len(df):,} box scores for EWMA computation.")
        return df

    except Exception as e:
        logger.error(f"[!] Failed to fetch league game log: {e}")
        return pd.DataFrame()
