# src/engine.py
import pandas as pd
from nba_fetcher import get_league_gamelog
from constants import SUPPORTED_STATS, STAT_MAPPING
from utils import logger, timer

@timer
def calculate_all_edges(pp_df, sample_size=15, edge_threshold=15.0):
    logger.info(f"\n[*] Booting up Math Engine for full board analysis...")
    
    supported_stats = [
        "Points", "Rebounds", "Assists", "Pts+Rebs+Asts",
        "3-PT Made", "Blocked Shots", "Steals", "Turnovers",
        "Blks+Stls", "Pts+Rebs", "Pts+Asts", "Rebs+Asts"
    ]
    
    player_props = pp_df[pp_df['Stat'].isin(supported_stats)].copy()
    
    if player_props.empty:
        logger.info("[-] No supported props found in the current PrizePicks board.")
        return pd.DataFrame()

    results = []
    
    # 1. FETCH ONCE (The God-Call)
    league_df = get_league_gamelog()
    if league_df is None or league_df.empty:
        logger.info("[!] Cannot proceed without league game logs.")
        return pd.DataFrame()

    unique_players = player_props['Player'].unique()
    logger.info(f"[*] Analyzing {len(unique_players)} players in memory... (This will be fast)")

    # 2. PROCESS IN MEMORY
    for index, row in player_props.iterrows():
        player = row['Player']
        stat_type = row['Stat']
        pp_line = row['Line']
        game_date = row['Game Date']
        
        # Slice the massive dataframe to just this player
        player_logs = league_df[league_df['PLAYER_NAME'] == player]
        
        # Safety check: Do they have enough games played this season?
        if player_logs.empty or len(player_logs) < 5:
            continue 
            
        recent_games = player_logs.head(sample_size).copy()
        
        # Map the PrizePicks string to the NBA DataFrame column
        if stat_type == "Pts+Rebs+Asts": calc_col = "PRA"
        elif stat_type == "Points": calc_col = "PTS"
        elif stat_type == "Rebounds": calc_col = "REB"
        elif stat_type == "Assists": calc_col = "AST"
        elif stat_type == "3-PT Made": calc_col = "FG3M"
        elif stat_type == "Blocked Shots": calc_col = "BLK"
        elif stat_type == "Steals": calc_col = "STL"
        elif stat_type == "Turnovers": calc_col = "TOV"
        elif stat_type == "Blks+Stls": calc_col = "BS"
        elif stat_type == "Pts+Rebs": calc_col = "PR"
        elif stat_type == "Pts+Asts": calc_col = "PA"
        elif stat_type == "Rebs+Asts": calc_col = "RA"
        else: continue 
        
        # CALCULATE LONG-TERM VS SHORT-TERM
        true_median_15 = recent_games[calc_col].median()
        true_mean_15 = recent_games[calc_col].mean()
        true_median_5 = recent_games[calc_col].head(5).median()
        
        raw_diff = true_median_15 - pp_line
        edge_percent = (raw_diff / pp_line) * 100
        play = "OVER" if raw_diff > 0 else "UNDER"
        
        # === THE TREND FILTER ===
        if play == "UNDER" and true_median_5 > (true_median_15 * 1.25):
            continue 
            
        if play == "OVER" and true_median_5 < (true_median_15 * 0.75):
            continue 
        
        if abs(edge_percent) >= edge_threshold:
            results.append({
                "Player": player,
                "Team": row['Team'],
                "Matchup": row['Matchup'],
                "Stat": stat_type,
                "PP Line": pp_line,
                "Game Date": game_date,
                "15g Median": true_median_15,
                "5g Median": true_median_5,
                "15g Avg": round(true_mean_15, 1),
                "Diff": raw_diff,
                "Edge %": round(edge_percent, 2),
                "Play": play
            })
            
    results_df = pd.DataFrame(results)
    
    if not results_df.empty:
        results_df['Abs Edge'] = results_df['Edge %'].abs()
        results_df = results_df.sort_values(by='Abs Edge', ascending=False)
        results_df = results_df.drop(columns=['Abs Edge'])
        
    return results_df