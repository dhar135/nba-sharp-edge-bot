# src/services/odds_api.py
"""
The Odds API integration for Vegas line comparison.

Fetches player prop lines from DraftKings, FanDuel, BetMGM, and other US sportsbooks
and builds a lookup table for comparing against PrizePicks lines.

Setup: Add ODDS_API_KEY to your .env file.
Free tier: 500 requests/month — sufficient for daily playoff usage.
Sign up at: https://the-odds-api.com
"""
import os
import requests
from utils.utils import logger, normalize_name

_BASE_URL = "https://api.the-odds-api.com/v4"
_SPORT = "basketball_nba"

# Sportsbooks to pull from — all available on free tier US region
_BOOKMAKERS = ["draftkings", "fanduel", "betmgm"]

# Mapping from PrizePicks stat names → Odds API market keys
# Only markets available on the free/standard player props tier
PP_STAT_TO_MARKET = {
    "Points":           "player_points",
    "Rebounds":         "player_rebounds",
    "Assists":          "player_assists",
    "Pts+Rebs+Asts":   "player_points_rebounds_assists",
    "Pts+Rebs":         "player_points_rebounds",
    "Pts+Asts":         "player_points_assists",
    "Rebs+Asts":        "player_rebounds_assists",
    "Blks+Stls":        "player_blocks_steals",
}

# Reverse mapping for quick lookup
MARKET_TO_PP_STAT = {v: k for k, v in PP_STAT_TO_MARKET.items()}


def _get_api_key():
    key = os.getenv("ODDS_API_KEY")
    if not key:
        logger.warning("[!] ODDS_API_KEY not set in .env — Vegas comparison disabled.")
    return key


def fetch_nba_events():
    """
    Fetches upcoming NBA events (game IDs, teams, start times).
    Does NOT count against quota.

    Returns:
        list of dicts: [{id, home_team, away_team, commence_time}, ...]
        Empty list if API key missing or request fails.
    """
    api_key = _get_api_key()
    if not api_key:
        return []

    url = f"{_BASE_URL}/sports/{_SPORT}/events"
    try:
        resp = requests.get(url, params={"apiKey": api_key}, timeout=10)
        remaining = resp.headers.get("x-requests-remaining", "?")
        logger.info(f"[*] Odds API: fetched events. Quota remaining: {remaining}")

        if resp.status_code == 401:
            logger.error("[!] Odds API: Invalid API key.")
            return []
        if resp.status_code != 200:
            logger.warning(f"[!] Odds API events failed: HTTP {resp.status_code}")
            return []

        return resp.json()

    except Exception as e:
        logger.warning(f"[!] Odds API events request failed: {e}")
        return []


def fetch_player_props(event_id, markets):
    """
    Fetches player prop lines for a specific game event from US sportsbooks.
    Costs 1 quota credit per market.

    Args:
        event_id: The Odds API event ID string
        markets: list of market keys, e.g. ['player_points', 'player_rebounds']

    Returns:
        list of outcome dicts from the API response bookmakers
    """
    api_key = _get_api_key()
    if not api_key:
        return []

    url = f"{_BASE_URL}/sports/{_SPORT}/events/{event_id}/odds"
    markets_str = ",".join(markets)

    try:
        resp = requests.get(url, params={
            "apiKey": api_key,
            "regions": "us",
            "markets": markets_str,
            "bookmakers": ",".join(_BOOKMAKERS),
            "oddsFormat": "american",
        }, timeout=15)

        remaining = resp.headers.get("x-requests-remaining", "?")
        cost = resp.headers.get("x-requests-last", "?")
        logger.info(f"[*] Odds API props for event {event_id[:8]}... | Cost: {cost} | Remaining: {remaining}")

        if resp.status_code == 422:
            logger.info(f"  [~] No player props available for event {event_id[:8]} yet.")
            return []
        if resp.status_code != 200:
            logger.warning(f"[!] Odds API props failed: HTTP {resp.status_code}")
            return []

        return resp.json().get("bookmakers", [])

    except Exception as e:
        logger.warning(f"[!] Odds API props request failed: {e}")
        return []


def build_vegas_lookup(events):
    """
    Main entry point: fetches player props for all upcoming games and builds
    a fast lookup dict for use in the pipeline.

    Process:
        1. For each event, fetch all supported player prop markets
        2. For each player+market, collect all bookmaker lines
        3. Compute the median line across bookmakers (consensus)
        4. Store in a lookup: {(player_name_lower, pp_stat_name): median_line}

    Args:
        events: list from fetch_nba_events()

    Returns:
        dict: {("player name lowercase", "PrizePicks Stat Name"): float median_line}
        Returns empty dict if API key missing or no events.
    """
    if not events:
        return {}

    api_key = _get_api_key()
    if not api_key:
        return {}

    # Only request markets we have a PP stat mapping for
    all_markets = list(PP_STAT_TO_MARKET.values())
    lookup = {}
    raw_lines = {}  # (player_lower, stat) → [line, line, ...] for median calculation

    for event in events:
        event_id = event.get("id")
        home = event.get("home_team", "")
        away = event.get("away_team", "")
        logger.info(f"[*] Fetching Vegas props: {away} @ {home}")

        bookmakers = fetch_player_props(event_id, all_markets)

        for bookmaker in bookmakers:
            bk_key = bookmaker.get("key", "")
            for market in bookmaker.get("markets", []):
                market_key = market.get("key", "")
                pp_stat = MARKET_TO_PP_STAT.get(market_key)
                if not pp_stat:
                    continue

                for outcome in market.get("outcomes", []):
                    # Odds API player prop outcomes have a "description" field for player name
                    # and a "point" field for the line value
                    player_name = normalize_name(outcome.get("description", outcome.get("name", "")))
                    point = outcome.get("point")

                    if not player_name or point is None:
                        continue

                    key = (player_name, pp_stat)
                    if key not in raw_lines:
                        raw_lines[key] = []
                    raw_lines[key].append(float(point))

    # Compute median line across all bookmakers for each player+stat
    for (player_lower, pp_stat), lines in raw_lines.items():
        if lines:
            sorted_lines = sorted(lines)
            n = len(sorted_lines)
            median = sorted_lines[n // 2] if n % 2 == 1 else (sorted_lines[n//2 - 1] + sorted_lines[n//2]) / 2
            lookup[(player_lower, pp_stat)] = round(median, 1)

    logger.info(f"[+] Vegas lookup built: {len(lookup)} player/stat pairs from {len(events)} games.")
    return lookup


def get_vegas_comparison(player_name, stat_type, pp_line, play, vegas_lookup):
    """
    Compares a single PrizePicks play against the Vegas consensus line.

    Args:
        player_name: Player's full name (will be lowercased for matching)
        stat_type: PrizePicks stat name (e.g. "Points")
        pp_line: The PrizePicks line value
        play: "OVER" or "UNDER"
        vegas_lookup: dict from build_vegas_lookup()

    Returns:
        (vegas_line, vegas_diff, vegas_confirms): 
            vegas_line: float or None if not found
            vegas_diff: float (pp_line - vegas_line), positive = PP is higher
            vegas_confirms: bool — True if Vegas line supports our play direction
    """
    if not vegas_lookup:
        return None, None, None

    player_lower = normalize_name(player_name)
    key = (player_lower, stat_type)

    vegas_line = vegas_lookup.get(key)
    if vegas_line is None:
        return None, None, None

    vegas_diff = round(pp_line - vegas_line, 1)

    # Vegas confirms our play if:
    # - UNDER: PP line is >= Vegas line (PP is generous, UNDER is easier to hit)
    # - OVER: PP line is <= Vegas line (PP is conservative, OVER is easier to hit)
    if play == "UNDER":
        vegas_confirms = pp_line >= vegas_line
    else:  # OVER
        vegas_confirms = pp_line <= vegas_line

    return vegas_line, vegas_diff, vegas_confirms
