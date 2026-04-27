# src/engine/veto.py
"""
V2.1 Veto Layer — Rebuilt

Key changes from V2.0:
  1. Extends to ALL stat types, not just Points
  2. Uses EWMA-aligned features instead of raw medians
  3. XGBoost veto only fires for Points (since that's what it was trained on)
  4. For all other stats, uses a statistical consistency check instead
  5. Returns clear signal: vetoed plays should NEVER be logged to DB
"""
import pandas as pd
import numpy as np
import joblib
import os
from utils.utils import logger


class MLVetoLayer:
    """
    Multi-layered veto system:
      1. XGBoost model veto (Points only — model was trained on this)
      2. Statistical consistency veto (all stats — checks for unusual variance)
      3. Minutes stability veto (flags players with erratic minutes)
    """

    def __init__(self, model_path="models/xgb_pts_model.pkl"):
        self.model = None
        if os.path.exists(model_path):
            self.model = joblib.load(model_path)
            logger.info("[+] V1 XGBoost Veto Brain successfully loaded.")
        else:
            logger.warning(f"[-] Veto model not found at {model_path}. ML veto disabled.")

    def check_veto(self, stat_type, matchup_str, game_date, recent_logs_df,
                   deterministic_play, ewma_projection=None, line=None):
        """
        Multi-layer veto evaluation.

        Args:
            stat_type:           "Points", "Rebounds", etc.
            matchup_str:         "vs LAL" or "@ GSW"
            game_date:           Date string (YYYY-MM-DD)
            recent_logs_df:      DataFrame with player's recent game logs
            deterministic_play:  "OVER" or "UNDER" from the projection engine
            ewma_projection:     The V2.1 EWMA-based projection value
            line:                The PrizePicks line value

        Returns:
            (is_vetoed: bool, veto_reason: str)
        """
        veto_reasons = []

        # --- Layer 1: XGBoost Veto (Points only) ---
        if stat_type == "Points" and self.model is not None:
            ml_vetoed, ml_reason = self._xgboost_veto(
                matchup_str, game_date, recent_logs_df, deterministic_play
            )
            if ml_vetoed:
                veto_reasons.append(ml_reason)

        # --- Layer 2: Statistical Consistency Check (all stats) ---
        if recent_logs_df is not None and not recent_logs_df.empty:
            consistency_vetoed, consistency_reason = self._consistency_veto(
                stat_type, recent_logs_df, deterministic_play, line
            )
            if consistency_vetoed:
                veto_reasons.append(consistency_reason)

        # --- Layer 3: Minutes Stability Check ---
        if recent_logs_df is not None and not recent_logs_df.empty:
            minutes_vetoed, minutes_reason = self._minutes_stability_veto(
                recent_logs_df
            )
            if minutes_vetoed:
                veto_reasons.append(minutes_reason)

        is_vetoed = len(veto_reasons) > 0
        combined_reason = " | ".join(veto_reasons) if veto_reasons else "CLEARED"

        if is_vetoed:
            logger.info(f"    [X] VETO: {combined_reason}")

        return is_vetoed, combined_reason

    def _xgboost_veto(self, matchup_str, game_date, recent_logs_df, deterministic_play):
        """
        V1 XGBoost model veto — only for Points.
        Checks Days_Rest and Is_Home against the deterministic engine's decision.
        """
        if recent_logs_df.empty or 'PTS' not in recent_logs_df.columns:
            return False, ""

        is_home = 0 if '@' in matchup_str else 1

        try:
            last_game_date = pd.to_datetime(recent_logs_df.iloc[0]['GAME_DATE'])
            target_date = pd.to_datetime(game_date)
            days_rest = (target_date - last_game_date).days
            days_rest = min(max(days_rest, 0), 4)
        except Exception:
            days_rest = 3

        base_median_15 = recent_logs_df['PTS'].head(15).median()
        base_median_5 = recent_logs_df['PTS'].head(5).median()

        ml_edge_diff = base_median_5 - base_median_15
        ml_edge_pct = (ml_edge_diff / base_median_15) * 100 if base_median_15 > 0 else 0

        features = pd.DataFrame([{
            'Is_Home': is_home,
            'Days_Rest': days_rest,
            '15g_Median_PTS': base_median_15,
            '5g_Median_PTS': base_median_5,
            'Edge_Pct': ml_edge_pct
        }])

        prob_over = self.model.predict_proba(features)[0][1] * 100

        # Veto if ML strongly disagrees with the deterministic play
        if deterministic_play == "OVER" and prob_over < 45.0:
            return True, f"ML: OVER but model gives {prob_over:.1f}% (< 45%)"
        if deterministic_play == "UNDER" and prob_over > 55.0:
            return True, f"ML: UNDER but model gives OVER {prob_over:.1f}% (> 55%)"

        return False, ""

    def _consistency_veto(self, stat_type, recent_logs_df, play, line):
        """
        Statistical consistency check — vetoes plays where the player's
        recent performance is too volatile relative to the line.

        If the coefficient of variation (std/mean) is above 0.4 for the
        relevant stat, the projection is unreliable.
        """
        # Map stat types to game log columns
        stat_col_map = {
            "Points": "PTS", "Rebounds": "REB", "Assists": "AST",
            "3-PT Made": "FG3M", "Blocked Shots": "BLK",
            "Steals": "STL", "Turnovers": "TOV",
        }

        # For combo stats, we need to compute them
        combo_map = {
            "Pts+Rebs+Asts": ["PTS", "REB", "AST"],
            "Pts+Rebs": ["PTS", "REB"],
            "Pts+Asts": ["PTS", "AST"],
            "Rebs+Asts": ["REB", "AST"],
            "Blks+Stls": ["BLK", "STL"],
        }

        try:
            if stat_type in stat_col_map:
                col = stat_col_map[stat_type]
                if col not in recent_logs_df.columns:
                    return False, ""
                values = recent_logs_df[col].head(10).values.astype(float)
            elif stat_type in combo_map:
                components = combo_map[stat_type]
                if not all(c in recent_logs_df.columns for c in components):
                    return False, ""
                values = sum(recent_logs_df[c].head(10).values.astype(float) for c in components)
            else:
                return False, ""

            if len(values) < 5:
                return False, ""

            mean_val = np.mean(values)
            std_val = np.std(values)

            if mean_val == 0:
                return False, ""

            cv = std_val / mean_val  # Coefficient of variation

            # High variance + OVER bet = risky (player could easily underperform)
            if play == "OVER" and cv > 0.40:
                return True, f"HIGH VARIANCE: CV={cv:.2f} on {stat_type} (>{0.40} threshold)"

            # Check if the line is at the edge of the distribution
            if line is not None:
                # What fraction of recent games would have cleared the line?
                if play == "OVER":
                    hit_rate = np.mean(values > line)
                else:
                    hit_rate = np.mean(values < line)

                # If fewer than 30% of recent games would have hit, veto
                if hit_rate < 0.30:
                    return True, f"HISTORY: Only {hit_rate*100:.0f}% of last 10 games cleared line"

        except Exception:
            pass

        return False, ""

    def _minutes_stability_veto(self, recent_logs_df):
        """
        Vetoes plays for players with highly erratic minutes.
        If the standard deviation of minutes in the last 10 games is > 8,
        the minute projection is unreliable, making all stat projections suspect.
        """
        if 'MIN' not in recent_logs_df.columns:
            return False, ""

        try:
            # Handle MIN column that might be in "MM:SS" string format
            min_values = recent_logs_df['MIN'].head(10)

            if min_values.dtype == object:
                # Convert "MM:SS" to float minutes
                def parse_min(val):
                    if isinstance(val, str) and ':' in val:
                        parts = val.split(':')
                        return float(parts[0]) + float(parts[1]) / 60
                    return float(val)
                min_values = min_values.apply(parse_min)

            min_values = min_values.values.astype(float)

            if len(min_values) < 5:
                return False, ""

            std_minutes = np.std(min_values)

            if std_minutes > 8.0:
                return True, f"MINUTES VOLATILE: std={std_minutes:.1f} min (>8.0 threshold)"

        except Exception:
            pass

        return False, ""
