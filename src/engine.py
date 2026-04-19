# src/engine.py
import pandas as pd
from nba_fetcher import get_player_gamelog

def calculate_all_edges(pp_df, sample_size=15, edge_threshold=15.0):
    """
    Compares PrizePicks lines to the player's true median across multiple stats.
    Only returns edges that exceed the defined threshold.
    """
    print(f"\n[*] Booting up Math Engine for full board analysis...")
    
    # Define supported stats
    supported_stats = ["Points", "Rebounds", "Assists", "Pts+Rebs+Asts"]
    
    # Filter the board to only include stats we support
    processable_board = pp_df[pp_df['Stat'].isin(supported_stats)].copy()
    
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
        
        # Pre-calculate PRA (Points + Rebounds + Assists) just in case they have a PRA line
        recent_games['PRA'] = recent_games['PTS'] + recent_games['REB'] + recent_games['AST']
        
        # Loop through each of the player's lines and calculate the edge
        for index, row in player_props.iterrows():
            stat_type = row['Stat']
            pp_line = row['Line']
            
            # Map the PrizePicks string to our Pandas column
            if stat_type == "Pts+Rebs+Asts":
                calc_col = "PRA"
            elif stat_type == "Points":
                calc_col = "PTS"
            elif stat_type == "Rebounds":
                calc_col = "REB"
            elif stat_type == "Assists":
                calc_col = "AST"
            else:
                continue 
            
            true_median = recent_games[calc_col].median()
            true_mean = recent_games[calc_col].mean()
            
            raw_diff = true_median - pp_line
            edge_percent = (raw_diff / pp_line) * 100
            
            # FILTER: Only save the play if the edge is massive
            if abs(edge_percent) >= edge_threshold:
                results.append({
                    "Player": player,
                    "Stat": stat_type,
                    "PP Line": pp_line,
                    "15g Median": true_median,
                    "15g Avg": round(true_mean, 1),
                    "Diff": raw_diff,
                    "Edge %": round(edge_percent, 2),
                    "Play": "OVER" if raw_diff > 0 else "UNDER"
                })
                
    return pd.DataFrame(results)