# src/nba_fetcher.py
"""
NBA data fetching layer.
All external API calls go through here — grader.py and main.py stay clean.
"""

import time
import pandas as pd
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import (
    playergamelog,
    leaguegamelog,
    leaguedashteamstats,
    scoreboardv3,
    boxscoretraditionalv2,
)

from utils.utils import logger, timer


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CURRENT_SEASON = "2025-26"

# PrizePicks uses non-standard abbreviations — map them to official NBA ones
PP_TO_NBA_ABBR = {
    "SA":   "SAS",
    "NY":   "NYK",
    "GS":   "GSW",
    "NO":   "NOP",
    "UTAH": "UTA",
    "WSH":  "WAS",
}

# Maps PrizePicks stat names to DataFrame column names
STAT_COLUMN_MAP = {
    "Points":        "PTS",
    "Rebounds":      "REB",
    "Assists":       "AST",
    "Pts+Rebs+Asts": "PRA",
    "Pts+Rebs":      "PR",
    "Pts+Asts":      "PA",
    "Rebs+Asts":     "RA",
    "3-PT Made":     "FG3M",
    "Blocked Shots": "BLK",
    "Steals":        "STL",
    "Turnovers":     "TOV",
    "Blks+Stls":     "BS",
}

# Same map but for opponent defensive stats (LeagueDashTeamStats Opponent mode)
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

# Module-level scoreboard cache — keyed by game_date string.
# KEY: Only successfully fetched dates are stored here. Failed fetches are NOT
# cached so they can be retried on the next grading cycle (no cache poisoning).
_SCOREBOARD_CACHE: dict[str, pd.DataFrame] = {}


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _get_player_id(player_name: str) -> int | None:
    """Return the NBA player ID for a full name, or None if not found."""
    matches = players.find_players_by_full_name(player_name)
    if not matches:
        logger.warning(f"[!] Player not found in static list: '{player_name}'")
        return None
    return matches[0]["id"]


def _normalize_team_abbr(abbr: str) -> str:
    """Convert a PrizePicks team abbreviation to the official NBA abbreviation."""
    return PP_TO_NBA_ABBR.get(abbr.upper(), abbr.upper())


def _get_team_id(abbr: str) -> int | None:
    """Return the NBA team ID for an abbreviation (handles PP non-standard abbrs)."""
    nba_abbr = _normalize_team_abbr(abbr)
    nba_teams = teams.get_teams()
    match = next((t for t in nba_teams if t["abbreviation"] == nba_abbr), None)
    if not match:
        logger.warning(
            f"[!] Team not found: '{abbr}' (resolved to '{nba_abbr}'). "
            f"Valid abbreviations: {sorted(t['abbreviation'] for t in nba_teams)}"
        )
        return None
    return match["id"]


def resolve_stat_value(row: pd.Series, stat_type: str) -> float | None:
    """
    Extract the numeric value for a given stat_type from a player box-score row.
    Returns None if the stat_type is unknown or the value is missing.
    """
    col = STAT_COLUMN_MAP.get(stat_type)
    if col is None:
        logger.warning(f"[!] Unknown stat_type: '{stat_type}'")
        return None
    try:
        return float(row[col])
    except (KeyError, TypeError, ValueError):
        return None


def _fetch_and_cache_scoreboard(game_date: str) -> pd.DataFrame:
    """
    Fetch the ScoreboardV3 for a date, extract home/away team IDs by parsing gameCode,
    and cache the result.

    WHY failures are NOT cached:
        A transient network error on the first call for a date would otherwise
        poison the cache and silently block all grading for that date on every
        subsequent cycle. By only caching successes (and empty-but-valid responses),
        the next grading run will retry the API call automatically.

    gameCode format: "20260424/PORSAS" — away tricode (POR) + home tricode (SAS).
    We extract these tricodes, then look them up in line_score to get teamIds.

    Args:
        game_date: Date string in YYYY-MM-DD format.

    Returns:
        DataFrame with home_team_id / away_team_id columns added,
        or an empty DataFrame if the fetch failed or no games were scheduled.
    """
    if game_date in _SCOREBOARD_CACHE:
        return _SCOREBOARD_CACHE[game_date]

    logger.info(f"[*] Fetching scoreboard for {game_date} (not yet cached)...")
    time.sleep(1)  # Rate-limit — one scoreboard call per unique game date

    try:
        sb = scoreboardv3.ScoreboardV3(game_date=game_date, timeout=15)

        header_df = sb.game_header.get_data_frame()
        line_df = sb.line_score.get_data_frame()

        if header_df.empty:
            logger.warning(f"[!] No games found for {game_date}.")
            _SCOREBOARD_CACHE[game_date] = pd.DataFrame()
            return pd.DataFrame()

        # line_score has 2 rows per game (one per team), no home/away label.
        # Parse gameCode "20260424/PORSAS" → away=POR, home=SAS
        header_df[['away_tricode', 'home_tricode']] = (
            header_df['gameCode']
            .str.split('/', expand=True)[1]  # "PORSAS"
            .apply(lambda s: pd.Series([s[:3], s[3:]]))  # ["POR", "SAS"]
        )

        # Map tricode → teamId using line_score
        tricode_to_id = line_df.set_index('teamTricode')['teamId'].to_dict()

        header_df['home_team_id'] = header_df['home_tricode'].map(tricode_to_id)
        header_df['away_team_id'] = header_df['away_tricode'].map(tricode_to_id)

        logger.info(
            f"[+] Scoreboard cached for {game_date}: {len(header_df)} game(s). "
            f"Game IDs: {header_df['gameId'].tolist()}"
        )

        _SCOREBOARD_CACHE[game_date] = header_df
        return header_df

    except Exception as e:
        logger.error(
            f"[!] Scoreboard fetch FAILED for {game_date}: {type(e).__name__}: {e}\n"
            f"    Bets for this date will remain PENDING and retry next cycle."
        )
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@timer
def get_player_gamelog(player_name: str, season: str = CURRENT_SEASON) -> pd.DataFrame | None:
    """
    Fetch the full game log for a single player for the given season.

    Args:
        player_name: Full player name, e.g. "LeBron James"
        season:      Season string, e.g. "2025-26"

    Returns:
        DataFrame of player game logs, or None on failure.
    """
    logger.info(f"[*] Fetching game log for: {player_name}")

    player_id = _get_player_id(player_name)
    if player_id is None:
        return None

    try:
        log = playergamelog.PlayerGameLog(player_id=player_id, season=season)
        df = log.get_data_frames()[0]
        time.sleep(0.6)
        return df
    except Exception as e:
        logger.error(f"[!] Failed to fetch game log for {player_name}: {e}")
        return None


@timer
def get_league_gamelog(season: str = CURRENT_SEASON) -> pd.DataFrame | None:
    """
    Fetch every player box score for the season in one call (the "God-Call").

    Pre-calculates all combo stat columns (PRA, PR, PA, RA, BS) vectorially
    so grader.py can look them up directly via STAT_COLUMN_MAP.

    Args:
        season: Season string, e.g. "2025-26"

    Returns:
        Sorted DataFrame with combo columns added, or None on failure.
    """
    logger.info(f"[*] Fetching league-wide game log for {season}...")
    try:
        log = leaguegamelog.LeagueGameLog(
            player_or_team_abbreviation="P",
            season=season,
            season_type_all_star="Playoffs",
        )
        df = log.get_data_frames()[0]

        df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
        df = df.sort_values(by=["PLAYER_NAME", "GAME_DATE"], ascending=[True, False])

        # Vectorised combo stats — computed once for the entire league
        df["PRA"] = df["PTS"] + df["REB"] + df["AST"]
        df["PR"]  = df["PTS"] + df["REB"]
        df["PA"]  = df["PTS"] + df["AST"]
        df["RA"]  = df["REB"] + df["AST"]
        df["BS"]  = df["BLK"] + df["STL"]

        logger.info(f"[+] Loaded {len(df):,} box scores into memory.")
        return df

    except Exception as e:
        logger.error(f"[!] Failed to fetch league game log: {e}")
        return None


@timer
def get_opponent_matchup_multipliers(season: str = CURRENT_SEASON) -> dict | None:
    """
    Build a per-team, per-stat defensive difficulty multiplier.

    Multiplier > 1.0  →  this team allows more of that stat than league average
    Multiplier < 1.0  →  this team is stingy on that stat

    Returns:
        Dict of shape { "LAL": { "Points": 1.05, "Rebounds": 0.97, ... }, ... }
        or None on failure.
    """
    logger.info(f"[*] Fetching defensive matchup multipliers for {season}...")
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

        # Build multiplier dict — one pass per stat avoids recomputing league_avg
        multipliers: dict[str, dict[str, float]] = {}
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
        return None


@timer
def get_game_status(player_name: str, game_date: str, cache: dict, team_abbr: str) -> str:
    """
    Determine the status of a player's game on a given date.

    Returns one of:
        "FINAL"       — game is over, stats are ready to be graded
        "IN_PROGRESS" — game is currently live
        "PRE_GAME"    — not started, or player/team/game could not be resolved

    Side-effects:
        Populates cache[player_name] with player_id, team_id, and game_id
        so grader.py can retrieve them without additional API calls.

    Args:
        player_name: Full player name
        game_date:   Date string in YYYY-MM-DD format
        cache:       Mutable dict shared across the grading loop (in-out param)
        team_abbr:   Team abbreviation from the DB (may be PP non-standard)
    """
    # 1. Resolve player ID — cached for the duration of the grading run
    if player_name not in cache:
        player_id = _get_player_id(player_name)
        if player_id is None:
            return "PRE_GAME"
        cache[player_name] = {"player_id": player_id}

    # 2. Resolve team ID — log the raw abbr so any mismatch is immediately visible
    team_id = _get_team_id(team_abbr)
    if team_id is None:
        logger.warning(
            f"[!] '{player_name}': could not resolve team ID for abbr '{team_abbr}'. "
            f"Check PP_TO_NBA_ABBR mapping in nba_fetcher.py."
        )
        return "PRE_GAME"
    cache[player_name]["team_id"] = team_id

    # 3. Fetch (or retrieve cached) scoreboard for the game date
    sb_df = _fetch_and_cache_scoreboard(game_date)
    if sb_df.empty:
        return "PRE_GAME"

    # 4. Find this team's game in the scoreboard
    game_row = sb_df[
        (sb_df["home_team_id"] == team_id) | (sb_df["away_team_id"] == team_id)
    ]
    if game_row.empty:
        logger.warning(
            f"[!] '{player_name}' (team_id={team_id}, abbr='{team_abbr}'): "
            f"not found in scoreboard for {game_date}.\n"
            f"    Home IDs on that date: {sb_df['home_team_id'].tolist()}\n"
            f"    Away IDs on that date: {sb_df['away_team_id'].tolist()}"
        )
        return "PRE_GAME"

    row = game_row.iloc[0]
    cache[player_name]["game_id"] = row["gameId"]

    # gameStatus: 1 = Scheduled, 2 = Live, 3 = Final
    status_id = row["gameStatus"]
    if status_id == 3:
        return "FINAL"
    elif status_id == 2:
        return "IN_PROGRESS"
    else:
        return "PRE_GAME"


@timer
def get_live_boxscore(game_id: str) -> pd.DataFrame:
    """
    Pull the traditional V2 box score for a finished (or live) game.

    Args:
        game_id: NBA game ID string from the scoreboard

    Returns:
        DataFrame of player stats, or an empty DataFrame on failure.
    """
    try:
        box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        return box.get_data_frames()[0]
    except Exception as e:
        logger.error(f"[!] Live boxscore failed for game {game_id}: {e}")
        return pd.DataFrame()