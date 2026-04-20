# src/nba_fetcher.py
import pandas as pd
from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog, leaguegamelog, leaguedashteamstats # NEW IMPORT
import time
from utils.utils import logger, timer
from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv2
from nba_api.stats.static import players, teams


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
    
@timer
def get_opponent_matchup_multipliers(season="2025-26"):
    """
    Fetches league-wide opponent stats and calculates a matchup multiplier.
    If a team allows 110 PTS (league avg 100), their PTS multiplier is 1.10.
    """
    logger.info(f"[*] Fetching Defensive Matchup Multipliers for {season}...")
    try:
        stats = leaguedashteamstats.LeagueDashTeamStats(
            measure_type_detailed_defense='Opponent',
            per_mode_detailed='PerGame',
            season=season
        )
        df = stats.get_data_frames()[0]
        
        # Map Team ID to Abbreviation (e.g. LAL, TOR)
        from nba_api.stats.static import teams
        nba_teams = teams.get_teams()
        team_dict = {t['id']: t['abbreviation'] for t in nba_teams}
        df['TEAM_ABBR'] = df['TEAM_ID'].map(team_dict)
        
        # === THE FIX: Use OPP_ prefixes for Opponent Stats ===
        df['OPP_PRA'] = df['OPP_PTS'] + df['OPP_REB'] + df['OPP_AST']
        df['OPP_PR'] = df['OPP_PTS'] + df['OPP_REB']
        df['OPP_PA'] = df['OPP_PTS'] + df['OPP_AST']
        df['OPP_RA'] = df['OPP_REB'] + df['OPP_AST']
        df['OPP_BS'] = df['OPP_BLK'] + df['OPP_STL']
        
        stat_mapping = {
            "Points": "OPP_PTS",
            "Rebounds": "OPP_REB",
            "Assists": "OPP_AST",
            "Pts+Rebs+Asts": "OPP_PRA",
            "3-PT Made": "OPP_FG3M",
            "Blocked Shots": "OPP_BLK", 
            "Steals": "OPP_STL",
            "Turnovers": "OPP_TOV",
            "Blks+Stls": "OPP_BS",
            "Pts+Rebs": "OPP_PR",
            "Pts+Asts": "OPP_PA",
            "Rebs+Asts": "OPP_RA"
        }
        
        multipliers = {}
        for stat_name, col in stat_mapping.items():
            league_avg = df[col].mean()
            for _, row in df.iterrows():
                team = row['TEAM_ABBR']
                if team not in multipliers:
                    multipliers[team] = {}
                
                # Calculate multiplier
                multipliers[team][stat_name] = round(row[col] / league_avg, 3) if league_avg > 0 else 1.0
                
        logger.info(f"[+] Generated defensive multipliers for {len(multipliers)} teams.")
        return multipliers
        
    except Exception as e:
        logger.error(f"[!] Failed to fetch defensive stats: {e}")
        return None
    
SCOREBOARD_CACHE = {}

@timer
def get_game_status(player_name, game_date, cache, team_abbr):
    """Finds the Game ID for a player's team on a specific date and checks if it is FINAL."""
    
    # 1. Get Player ID
    if player_name not in cache:
        player_info = players.find_players_by_full_name(player_name)
        if not player_info:
            return "PRE_GAME"
        cache[player_name] = {'player_id': player_info[0]['id']}
    
    # 2. Get Team ID (Handling PrizePicks abbreviations)
    nba_teams = teams.get_teams()
    pp_to_nba = {"SA": "SAS", "NY": "NYK", "GS": "GSW", "NO": "NOP", "UTAH": "UTA", "WSH": "WAS"}
    nba_abbr = pp_to_nba.get(team_abbr.upper(), team_abbr.upper())
    
    team_info = [t for t in nba_teams if t['abbreviation'] == nba_abbr]
    if not team_info:
        return "PRE_GAME"
    team_id = team_info[0]['id']
    cache[player_name]['team_id'] = team_id

    # 3. Fetch Scoreboard for the Date
    if game_date not in SCOREBOARD_CACHE:
        try:
            sb = scoreboardv2.ScoreboardV2(game_date=game_date)
            SCOREBOARD_CACHE[game_date] = sb.get_data_frames()[0]
        except Exception as e:
            logger.error(f"[!] Failed to fetch scoreboard for {game_date}: {e}")
            return "PRE_GAME"

    sb_df = SCOREBOARD_CACHE[game_date]
    if sb_df.empty:
        return "PRE_GAME"

    # 4. Find the specific game and check status
    game_row = sb_df[(sb_df['HOME_TEAM_ID'] == team_id) | (sb_df['VISITOR_TEAM_ID'] == team_id)]
    if game_row.empty:
        return "PRE_GAME" 
        
    cache[player_name]['game_id'] = game_row.iloc[0]['GAME_ID']
    status_id = game_row.iloc[0]['GAME_STATUS_ID'] # 1=Pre, 2=Live, 3=Final
    
    if status_id == 3: return "FINAL"
    elif status_id == 2: return "IN_PROGRESS"
    else: return "PRE_GAME"

@timer
def get_live_boxscore(game_id):
    """Pulls the real-time V2 Boxscore for a game that just finished."""
    try:
        box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        return box.get_data_frames()[0]
    except Exception as e:
        logger.error(f"[!] Live Boxscore failed for game {game_id}: {e}")
        return pd.DataFrame()