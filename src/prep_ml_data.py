# src/prep_ml_data.py
import pandas as pd
import numpy as np
from nba_api.stats.endpoints import leaguegamelog
import time
import os
from utils import logger, timer

@timer
def create_ml_dataset():
    # We will train on the last two full seasons
    seasons = ["2023-24", "2024-25"]
    logger.info(f"[*] Fetching historical data for seasons: {seasons}...")
    
    all_dfs = []
    for season in seasons:
        try:
            log = leaguegamelog.LeagueGameLog(player_or_team_abbreviation='P', season=season)
            df = log.get_data_frames()[0]
            all_dfs.append(df)
            time.sleep(1) # Be polite to the API
        except Exception as e:
            logger.error(f"Failed to fetch {season}: {e}")
            
    if not all_dfs:
        logger.error("[-] No data fetched.")
        return
        
    df = pd.concat(all_dfs)
    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
    
    # Sort chronologically so rolling windows don't look into the future!
    df = df.sort_values(['PLAYER_ID', 'GAME_DATE'])
    
    logger.info("[*] Calculating rolling features (This takes a moment)...")
    
    # --- FEATURE ENGINEERING ---
    
    # 1. The Pseudo-Line: We pretend the 15g Median was the PrizePicks line that day
    # .shift(1) ensures we don't include the game we are trying to predict!
    df['15g_Median_PTS'] = df.groupby('PLAYER_ID')['PTS'].transform(lambda x: x.shift(1).rolling(15, min_periods=15).median())
    df['5g_Median_PTS'] = df.groupby('PLAYER_ID')['PTS'].transform(lambda x: x.shift(1).rolling(5, min_periods=5).median())
    
    # 2. Context Features
    df['Prev_Game_Date'] = df.groupby('PLAYER_ID')['GAME_DATE'].shift(1)
    df['Days_Rest'] = (df['GAME_DATE'] - df['Prev_Game_Date']).dt.days
    df['Days_Rest'] = df['Days_Rest'].fillna(3).clip(upper=4) # Cap at 4+ days of rest
    df['Is_Home'] = (~df['MATCHUP'].str.contains('@')).astype(int) # 1 if Home, 0 if Away
    
    # Drop rows where we don't have enough history (a rookie's first 14 games)
    ml_df = df.dropna(subset=['15g_Median_PTS']).copy()
    
    # 3. The Math Engine Edge
    ml_df['Edge_Diff'] = ml_df['5g_Median_PTS'] - ml_df['15g_Median_PTS']
    ml_df['Edge_Pct'] = (ml_df['Edge_Diff'] / ml_df['15g_Median_PTS']).replace([np.inf, -np.inf], 0).fillna(0) * 100
    
    # --- TARGET VARIABLE (The Answer Key) ---
    # Did they score more points than their 15g median?
    ml_df['Hit_Over'] = (ml_df['PTS'] > ml_df['15g_Median_PTS']).astype(int)
    
    # Clean up the final DataFrame
    features = ['PLAYER_NAME', 'GAME_DATE', 'MATCHUP', 'Is_Home', 'Days_Rest', 
                '15g_Median_PTS', '5g_Median_PTS', 'Edge_Pct', 'PTS', 'Hit_Over']
    ml_df = ml_df[features]
    
    # Save it
    out_dir = "data"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
        
    out_path = os.path.join(out_dir, "ml_dataset_pts.csv")
    ml_df.to_csv(out_path, index=False)
    
    logger.info(f"[+] Generated massive ML dataset with {len(ml_df)} simulated bets!")
    logger.info(f"[*] Saved to {out_path}")
    
    # Baseline Sanity Check
    win_rate = ml_df['Hit_Over'].mean() * 100
    logger.info(f"[*] League-wide Baseline OVER rate: {win_rate:.2f}%")

if __name__ == "__main__":
    create_ml_dataset()