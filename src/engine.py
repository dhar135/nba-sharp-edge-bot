# src/engine.py
import pandas as pd
from nba_fetcher import get_player_gamelog
from constants import SUPPORTED_STATS, STAT_MAPPING

def calculate_all_edges(pp_df, sample_size=15, edge_threshold=15.0):
    """
    Compares PrizePicks lines to the player's true median across multiple stats.
    Only returns edges that exceed the defined threshold.
    """
    print("\n[*] Booting up Math Engine for full board analysis...")
    
    # Filter the board to only include stats we support
    processable_board = pp_df[pp_df['Stat'].isin(SUPPORTED_STATS)].copy()
    
    # OPTIMIZATION: Get unique players to avoid duplicate API calls
    players_to_check = processable_board['Player'].unique()
    print(f"[*] Found {len(players_to_check)} unique players. Fetching logs (this takes ~1-2 minutes)...")
    
    results = []
    
    for player in players_to_check:
        # Get all PrizePicks props for this specific player
        player_props = processable_board[processable_board['Player'] == player]
        
        # Fetch NBA data exactly ONCE per player
        df = get_player_gamelog(player)
        
        if df is None or df.empty:
            continue
            
        recent_games = df.head(sample_size).copy()
        
        # Pre-calculate combo stats needed for PrizePicks props
        recent_games['PRA'] = recent_games['PTS'] + recent_games['REB'] + recent_games['AST']
        recent_games['PR'] = recent_games['PTS'] + recent_games['REB']
        recent_games['PA'] = recent_games['PTS'] + recent_games['AST']
        recent_games['RA'] = recent_games['REB'] + recent_games['AST']
        recent_games['BS'] = recent_games['BLK'] + recent_games['STL']
        
        # Loop through each of the player's lines and calculate the edge
        for index, row in player_props.iterrows():
            stat_type = row['Stat']
            pp_line = row['Line']
            game_date = row['Game Date'] # NEW: Extract from row
            
            # Map the PrizePicks string to our Pandas column
            calc_col = STAT_MAPPING.get(stat_type)
            if not calc_col:
                continue 
            
            # CALCULATE LONG-TERM VS SHORT-TERM
            true_median_15 = recent_games[calc_col].median()
            true_mean_15 = recent_games[calc_col].mean()
            true_median_5 = recent_games[calc_col].head(5).median() # NEW: Short-term trend
            
            raw_diff = true_median_15 - pp_line
            edge_percent = (raw_diff / pp_line) * 100
            play = "OVER" if raw_diff > 0 else "UNDER"
            
            # === THE TREND FILTER ===
            # Protect against "Heaters" (Bot says UNDER, but player has been crushing it recently)
            if play == "UNDER" and true_median_5 > (true_median_15 * 1.25):
                continue # Skip the trap
                
            # Protect against "Injuries/Slumps" (Bot says OVER, but player has been terrible/limited recently)
            if play == "OVER" and true_median_5 < (true_median_15 * 0.75):
                continue # Skip the trap
            
            # FILTER: Only save the play if the edge is massive
            if abs(edge_percent) >= edge_threshold:
                results.append({
                    "Player": player,
                    "Team": row['Team'],          # NEW
                    "Matchup": row['Matchup'],    # NEW
                    "Stat": stat_type,
                    "PP Line": pp_line,
                    "Game Date": game_date,
                    "15g Median": true_median_15,
                    "5g Median": true_median_5, # NEW: Pass down the pipeline
                    "15g Avg": round(true_mean_15, 1),
                    "Diff": raw_diff,
                    "Edge %": round(edge_percent, 2),
                    "Play": "OVER" if raw_diff > 0 else "UNDER"
                })
                
    return pd.DataFrame(results)