# src/engine/veto.py
"""
Phase 3: ML Veto Layer (Sanity Check)
The XGBoost model serves as a final gatekeeper for situational factors.
While our V2 Poisson engine handles pace/usage/efficiency perfectly,
it's blind to fatigue (back-to-backs, travel). This layer fills that gap.
"""
import pandas as pd
import joblib
import os
from utils.utils import logger


class MLVetoLayer:
    """
    Loads the V1 XGBoost model to veto plays based on schedule context.
    Checks Days_Rest and Is_Home against the deterministic engine's decision.
    """
    
    def __init__(self, model_path="models/xgb_pts_model.pkl"):
        """
        Loads the V1 XGBoost model into memory to act as a secondary filter.
        """
        self.model = None
        if os.path.exists(model_path):
            self.model = joblib.load(model_path)
            logger.info("[+] V1 XGBoost Veto Brain successfully loaded.")
        else:
            logger.warning(f"[-] Veto model not found at {model_path}. Veto layer disabled.")

    def check_veto(self, stat_type, matchup_str, game_date, recent_logs_df, deterministic_play):
        """
        Evaluates the situational schedule (Rest/Location) against the deterministic play.
        
        Args:
        - stat_type: "Points", "Rebounds", etc. (Veto only applies to Points)
        - matchup_str: "vs LAL" or "@ GSW" format (@ indicates away)
        - game_date: String date of the game (format: 'YYYY-MM-DD')
        - recent_logs_df: DataFrame with player's recent games (PTS column required)
        - deterministic_play: "OVER" or "UNDER" from the Poisson engine
        
        Returns: 
        - (is_vetoed: bool, ml_prob: float or "-")
        """
        # V1 Model was exclusively trained on Points props
        if self.model is None or stat_type != "Points":
            return False, "-"
        
        # 1. Extract Situational Context (The main value of the Veto Layer)
        is_home = 0 if '@' in matchup_str else 1
        
        try:
            # Requires recent_logs_df to be sorted chronologically (Newest first)
            last_game_date = pd.to_datetime(recent_logs_df.iloc[0]['GAME_DATE'])
            target_date = pd.to_datetime(game_date)
            days_rest = (target_date - last_game_date).days
            days_rest = min(max(days_rest, 0), 4)  # Cap at 4 exactly as V1 was trained
        except Exception:
            days_rest = 3  # Default to rested if data is missing

        # 2. Reconstruct V1 Math Features to prevent model drift
        if recent_logs_df.empty or 'PTS' not in recent_logs_df.columns:
            return False, "-"

        base_median_15 = recent_logs_df['PTS'].head(15).median()
        base_median_5 = recent_logs_df['PTS'].head(5).median()
        
        ml_edge_diff = base_median_5 - base_median_15
        ml_edge_pct = (ml_edge_diff / base_median_15) * 100 if base_median_15 > 0 else 0

        # 3. Build the exact feature matrix the .pkl file expects
        features = pd.DataFrame([{
            'Is_Home': is_home,
            'Days_Rest': days_rest,
            '15g_Median_PTS': base_median_15,
            '5g_Median_PTS': base_median_5,
            'Edge_Pct': ml_edge_pct
        }])

        # 4. Predict probability of hitting the OVER
        prob_over = self.model.predict_proba(features)[0][1] * 100
        
        # 5. Execute Veto Logic
        is_vetoed = False
        
        # If Poisson says OVER, but ML says probability is < 50%, VETO.
        if deterministic_play == "OVER" and prob_over < 50.0:
            is_vetoed = True
            logger.info(f"    [X] VETO: Poisson generated OVER, but ML context gives it {round(prob_over, 1)}%")
            
        # If Poisson says UNDER, but ML says probability of OVER is > 50%, VETO.
        elif deterministic_play == "UNDER" and prob_over > 50.0:
            is_vetoed = True
            logger.info(f"    [X] VETO: Poisson generated UNDER, but ML context gives OVER {round(prob_over, 1)}%")

        return is_vetoed, round(prob_over, 1)
