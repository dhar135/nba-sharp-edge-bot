# src/engine.py
import pandas as pd
import numpy as np
import joblib
import os
from nba_fetcher import get_league_gamelog, get_opponent_matchup_multipliers
from utils.utils import logger, timer

@timer
def calculate_all_edges(pp_df, sample_size=15, edge_threshold=15.0):
    logger.info(f"\n[*] Booting up Math & ML Engine for full board analysis...")
    
    supported_stats = [
        "Points", "Rebounds", "Assists", "Pts+Rebs+Asts",
        "3-PT Made", "Blocked Shots", "Steals", "Turnovers",
        "Blks+Stls", "Pts+Rebs", "Pts+Asts", "Rebs+Asts"
    ]
    
    player_props = pp_df[pp_df['Stat'].isin(supported_stats)].copy()
    
    if player_props.empty:
        logger.info("[-] No supported props found.")
        return pd.DataFrame()

    results = []
    
    # 1. FETCH DATA & MODELS
    league_df = get_league_gamelog()
    opp_multipliers = get_opponent_matchup_multipliers()
    
    # Load the XGBoost Brain
    pts_model = None
    model_path = "models/xgb_pts_model.pkl"
    if os.path.exists(model_path):
        pts_model = joblib.load(model_path)
        logger.info("[+] ML Brain successfully loaded into memory.")
    
    if league_df is None or league_df.empty:
        return pd.DataFrame()

    # 2. PROCESS IN MEMORY
    for index, row in player_props.iterrows():
        player = row['Player']
        stat_type = row['Stat']
        pp_line = row['Line']
        game_date = row['Game Date']
        matchup_str = str(row['Matchup']).strip()
        
        opp_team_raw = matchup_str.split(' ')[-1].upper()
        pp_to_nba_map = {"SA": "SAS", "NY": "NYK", "GS": "GSW", "NO": "NOP", "UTAH": "UTA", "WSH": "WAS"}
        opp_team = pp_to_nba_map.get(opp_team_raw, opp_team_raw)
        
        multiplier = 1.0
        if opp_multipliers and opp_team in opp_multipliers:
            multiplier = opp_multipliers[opp_team].get(stat_type, 1.0)
        
        player_logs = league_df[league_df['PLAYER_NAME'] == player]
        if player_logs.empty or len(player_logs) < 5:
            continue 
            
        recent_games = player_logs.head(sample_size).copy()
        
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
        
        # CALCULATE BASE MEDIANS
        base_median_15 = recent_games[calc_col].median()
        base_mean_15 = recent_games[calc_col].mean()
        base_median_5 = recent_games[calc_col].head(5).median()
        
        adj_median_15 = base_median_15 * multiplier
        adj_median_5 = base_median_5 * multiplier
        adj_mean_15 = base_mean_15 * multiplier
        
        raw_diff = adj_median_15 - pp_line
        edge_percent = (raw_diff / pp_line) * 100
        play = "OVER" if raw_diff > 0 else "UNDER"
        
        # === PHASE 4: THE ML PROBABILITY ENGINE ===
        ml_prob = None
        if stat_type == "Points" and pts_model is not None:
            # Reconstruct the exact features we trained on
            is_home = 0 if '@' in matchup_str else 1
            
            try:
                last_game_date = recent_games.iloc[0]['GAME_DATE']
                target_date = pd.to_datetime(game_date)
                days_rest = (target_date - last_game_date).days
                days_rest = min(max(days_rest, 0), 4) # Cap at 4 just like training
            except:
                days_rest = 3
                
            ml_edge_diff = base_median_5 - base_median_15
            ml_edge_pct = (ml_edge_diff / base_median_15) * 100 if base_median_15 > 0 else 0
            
            features = pd.DataFrame([{
                'Is_Home': is_home,
                'Days_Rest': days_rest,
                '15g_Median_PTS': base_median_15,
                '5g_Median_PTS': base_median_5,
                'Edge_Pct': ml_edge_pct
            }])
            
            # Predict the probability of hitting the OVER
            prob_over = pts_model.predict_proba(features)[0][1] * 100
            
            # If the ML strongly disagrees with the Math Heuristic, skip the bet!
            if play == "OVER" and prob_over < 50.0:
                continue
            if play == "UNDER" and prob_over > 50.0:
                continue
                
            ml_prob = round(prob_over, 1)

        # Standard Trend Filter
        if play == "UNDER" and adj_median_5 > (adj_median_15 * 1.25): continue 
        if play == "OVER" and adj_median_5 < (adj_median_15 * 0.75): continue 
        
        if abs(edge_percent) >= edge_threshold:
            results.append({
                "Player": player,
                "Team": row['Team'],
                "Matchup": f"{matchup_str} (x{multiplier})",
                "Stat": stat_type,
                "PP Line": pp_line,
                "Game Date": game_date,
                "15g Median": round(adj_median_15, 1),
                "5g Median": round(adj_median_5, 1),
                "15g Avg": round(adj_mean_15, 1),
                "Diff": round(raw_diff, 1),
                "Edge %": round(edge_percent, 2),
                "Play": play,
                "ML Prob": ml_prob # NEW: Pass the AI's confidence to Discord
            })
            
    results_df = pd.DataFrame(results)
    
    if not results_df.empty:
        # Sort by the largest discrepancies (Absolute Edge)
        results_df['Abs Edge'] = results_df['Edge %'].abs()
        results_df = results_df.sort_values(by='Abs Edge', ascending=False)
        results_df = results_df.drop(columns=['Abs Edge'])
        
        # Clean up the NaN values for non-Points props
        if 'ML Prob' in results_df.columns:
            results_df['ML Prob'] = results_df['ML Prob'].fillna("-")
            
        # === NEW: Export to CSV ===
        out_dir = "data"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            
        csv_path = os.path.join(out_dir, "daily_picks.csv")
        results_df.to_csv(csv_path, index=False)
        logger.info(f"[+] Exported {len(results_df)} plays to {csv_path} sorted by Edge %")
        
    return results_df