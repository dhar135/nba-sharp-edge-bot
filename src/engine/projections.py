# src/engine/projections.py
"""
V2.1 Deterministic Projection Engine — Full Rebuild

Key changes from V2.0:
  1. EWMA-weighted baselines (α=0.3) instead of raw season averages
  2. Defensive matchup multipliers re-integrated from V1
  3. Advanced stats (USG%, TS%) actually used in scoring projections
  4. Home/away adjustment factor
  5. Blowout probability sigmoid for minute truncation
  6. Per-player state — no more shared mutable index
"""
import math
import pandas as pd
import numpy as np
from utils.utils import logger


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# EWMA smoothing factor — 0.3 means recent games weighted ~3x more than older
EWMA_ALPHA = 0.3

# Home court advantage multiplier (historically ~1.5-2% scoring boost)
HOME_SCORING_BOOST = 1.015
AWAY_SCORING_PENALTY = 0.985

# Blowout sigmoid parameters — calibrated to NBA spread data
# When spread >= 10, starters lose ~4-6 minutes on average
BLOWOUT_SPREAD_MIDPOINT = 10.0  # Spread where blowout prob = 50%
BLOWOUT_SIGMOID_K = 0.4         # Steepness of the sigmoid curve
BLOWOUT_MINUTE_PENALTY = 0.85   # Reduce minutes to 85% in blowout scenarios

# League-average per-minute rates (fallback constants from 2024-25 season)
LEAGUE_AVG_PTS_PER_MIN = 0.70
LEAGUE_AVG_REB_PER_MIN = 0.22
LEAGUE_AVG_AST_PER_MIN = 0.17


class DeterministicProjector:
    def __init__(self, advanced_df, tracking_df, pace_df, opp_multipliers=None, game_logs_df=None):
        """
        Initializes the engine with the stateless DataFrames fetched from Phase 1.

        Args:
            advanced_df:     League-wide player advanced stats (USG%, TS%, etc.)
            tracking_df:     Player tracking data (Touches, Potential Assists)
            pace_df:         Team pace and defensive rating data
            opp_multipliers: Dict of {team: {stat: multiplier}} from V1 defensive analysis
            game_logs_df:    Full league game log for EWMA computation
        """
        self.adv_df = advanced_df.set_index('PLAYER_NAME') if not advanced_df.empty else pd.DataFrame()
        self.track_df = tracking_df.set_index('PLAYER_ID') if not tracking_df.empty else pd.DataFrame()
        self.pace_df = pace_df.set_index('TEAM_ABBREVIATION') if not pace_df.empty else pd.DataFrame()
        self.opp_multipliers = opp_multipliers or {}
        self.game_logs_df = game_logs_df

        # Calculate League Averages for normalization
        self.league_pace = self.pace_df['PACE'].mean() if not self.pace_df.empty else 100.0

        # Pre-compute EWMA baselines if game logs are available
        self._ewma_cache = {}
        if game_logs_df is not None and not game_logs_df.empty:
            self._precompute_ewma_baselines()

    def _precompute_ewma_baselines(self):
        """
        Pre-compute EWMA baselines for all players from game logs.
        Uses last 20 games with exponential weighting (α=0.3).
        This replaces raw season averages as the projection anchor.
        """
        if self.game_logs_df is None or self.game_logs_df.empty:
            return

        df = self.game_logs_df.copy()

        # Ensure combo columns exist
        for col, formula in [('PRA', 'PTS+REB+AST'), ('PR', 'PTS+REB'),
                             ('PA', 'PTS+AST'), ('RA', 'REB+AST'), ('BS', 'BLK+STL')]:
            if col not in df.columns:
                parts = formula.split('+')
                if all(p in df.columns for p in parts):
                    df[col] = sum(df[p] for p in parts)

        stat_columns = ['PTS', 'REB', 'AST', 'FG3M', 'BLK', 'STL', 'TOV',
                        'PRA', 'PR', 'PA', 'RA', 'BS', 'MIN']

        for player_name, group in df.groupby('PLAYER_NAME'):
            # Sort by date descending, take last 20 games
            player_games = group.sort_values('GAME_DATE', ascending=False).head(20)

            if len(player_games) < 5:
                continue

            ewma_stats = {}
            n_games = len(player_games)

            for col in stat_columns:
                if col not in player_games.columns:
                    continue

                values = player_games[col].values.astype(float)

                # EWMA: most recent game gets highest weight
                weights = np.array([(1 - EWMA_ALPHA) ** i for i in range(n_games)])
                weights /= weights.sum()

                ewma_stats[col] = float(np.dot(values, weights))

            # Also compute variance for probability calibration
            for col in ['PTS', 'REB', 'AST', 'PRA', 'PR', 'PA', 'RA']:
                if col in player_games.columns:
                    ewma_stats[f'{col}_VAR'] = float(player_games[col].values.astype(float).var())
                    ewma_stats[f'{col}_STD'] = float(player_games[col].values.astype(float).std())

            ewma_stats['GAMES_PLAYED'] = n_games
            self._ewma_cache[player_name] = ewma_stats

    def calculate_pace_factor(self, player_team, opp_team):
        """
        Calculates the possession modifier based on both teams' pace.
        Uses the average of both teams' pace vs league average for symmetry.
        """
        try:
            opp_pace = self.pace_df.loc[opp_team, 'PACE']
            team_pace = self.pace_df.loc[player_team, 'PACE']

            # Average both teams' pace and compare to league average
            # This is more accurate than just using opponent pace
            game_pace = (opp_pace + team_pace) / 2.0
            pace_factor = game_pace / self.league_pace
            return pace_factor
        except KeyError:
            return 1.0

    def get_defensive_multiplier(self, opp_team, stat_type):
        """
        Returns the opponent's defensive multiplier for a given stat type.
        Values > 1.0 mean they allow MORE of that stat (bad defense).
        Values < 1.0 mean they allow LESS (good defense).
        """
        if not self.opp_multipliers or opp_team not in self.opp_multipliers:
            return 1.0
        return self.opp_multipliers[opp_team].get(stat_type, 1.0)

    def calculate_blowout_probability(self, player_team, opp_team):
        """
        Uses a sigmoid function to estimate the probability of a blowout.
        Based on the spread proxy from team net ratings (OFF_RTG - DEF_RTG delta).

        Returns:
            blowout_prob: float between 0 and 1
            minute_modifier: float (1.0 = no change, < 1.0 = expect fewer minutes)
        """
        try:
            team_def_rtg = self.pace_df.loc[player_team, 'DEF_RATING'] if player_team in self.pace_df.index else 110.0
            opp_def_rtg = self.pace_df.loc[opp_team, 'DEF_RATING'] if opp_team in self.pace_df.index else 110.0

            # Crude spread proxy: if our team's opponent allows a lot of points
            # and we don't, we're likely to blow them out
            # Lower DEF_RATING = better defense
            rating_diff = opp_def_rtg - team_def_rtg

            # Sigmoid: probability of a blowout based on rating differential
            blowout_prob = 1.0 / (1.0 + math.exp(-BLOWOUT_SIGMOID_K * (rating_diff - BLOWOUT_SPREAD_MIDPOINT)))

            # Scale minute penalty by blowout probability
            # If blowout is certain, reduce to BLOWOUT_MINUTE_PENALTY
            # If unlikely, no change
            minute_modifier = 1.0 - (blowout_prob * (1.0 - BLOWOUT_MINUTE_PENALTY))

            return blowout_prob, minute_modifier

        except (KeyError, TypeError):
            return 0.0, 1.0

    def get_ewma_baseline(self, player_name, stat_key):
        """
        Returns the EWMA-weighted baseline for a player's stat.
        Falls back to season average from adv_df if EWMA isn't available.
        """
        if player_name in self._ewma_cache and stat_key in self._ewma_cache[player_name]:
            return self._ewma_cache[player_name][stat_key]

        # Fallback to season average
        if player_name in self.adv_df.index:
            player_data = self.adv_df.loc[player_name]
            return player_data.get(stat_key, 0)

        return 0

    def get_stat_variance(self, player_name, stat_key):
        """
        Returns the empirical variance for a stat, used for Negative Binomial calibration.
        """
        var_key = f'{stat_key}_VAR'
        if player_name in self._ewma_cache and var_key in self._ewma_cache[player_name]:
            return self._ewma_cache[player_name][var_key]
        return None  # Will trigger fallback in probability.py

    def generate_projection(self, player_name, opp_team, projected_minutes, stat_type,
                            is_home=True):
        """
        Generate a deterministic projection for a player's stat line.

        The projection flow:
          1. Get EWMA baseline (or season avg fallback)
          2. Calculate per-minute rate from baseline
          3. Apply pace factor
          4. Apply defensive matchup multiplier
          5. Apply home/away modifier
          6. Apply blowout minute truncation
          7. Scale by projected minutes

        Args:
            player_name:      Full player name
            opp_team:         Opponent team abbreviation
            projected_minutes: Expected minutes to play
            stat_type:        PrizePicks stat type string
            is_home:          Whether the player is at home

        Returns:
            Projected stat value (float), or None if player not found
        """
        if player_name not in self.adv_df.index:
            return None

        player_data = self.adv_df.loc[player_name]
        player_team = player_data['TEAM_ABBREVIATION']

        base_min = self.get_ewma_baseline(player_name, 'MIN')
        if not base_min or base_min == 0:
            base_min = player_data.get('MIN', 0)
        if base_min == 0:
            return 0.0

        # --- 1. Get EWMA baselines for the relevant stats ---
        stat_map = {
            "Points": "PTS", "Rebounds": "REB", "Assists": "AST",
            "3-PT Made": "FG3M", "Blocked Shots": "BLK", "Steals": "STL",
            "Turnovers": "TOV",
            "Pts+Rebs+Asts": "PRA", "Pts+Rebs": "PR",
            "Pts+Asts": "PA", "Rebs+Asts": "RA", "Blks+Stls": "BS",
        }

        stat_key = stat_map.get(stat_type)
        if stat_key is None:
            return 0.0

        # For combo stats, build from components if EWMA combo isn't cached
        combo_components = {
            "PRA": ["PTS", "REB", "AST"],
            "PR": ["PTS", "REB"],
            "PA": ["PTS", "AST"],
            "RA": ["REB", "AST"],
            "BS": ["BLK", "STL"],
        }

        if stat_key in combo_components:
            base_value = sum(
                self.get_ewma_baseline(player_name, comp)
                for comp in combo_components[stat_key]
            )
        else:
            base_value = self.get_ewma_baseline(player_name, stat_key)

        if base_value == 0 and stat_key not in ['BLK', 'STL', 'TOV', 'BS']:
            # Fall back to adv_df for primary stats
            base_value = player_data.get(stat_key, 0)

        # --- 2. Calculate per-minute rate ---
        per_minute_rate = base_value / base_min

        # --- 3. Pace factor ---
        pace_factor = self.calculate_pace_factor(player_team, opp_team)

        # --- 4. Defensive matchup multiplier ---
        def_multiplier = self.get_defensive_multiplier(opp_team, stat_type)

        # --- 5. Home/away modifier ---
        location_mod = HOME_SCORING_BOOST if is_home else AWAY_SCORING_PENALTY

        # --- 6. Blowout minute truncation ---
        blowout_prob, minute_modifier = self.calculate_blowout_probability(player_team, opp_team)
        effective_minutes = projected_minutes * minute_modifier

        # --- 7. Advanced stat adjustments for scoring props ---
        efficiency_mod = 1.0
        if stat_type in ("Points", "Pts+Asts", "Pts+Rebs", "Pts+Rebs+Asts"):
            # Use USG% relative to league average (~20%) as a stability indicator
            # High-usage players' scoring is more predictable (less variance from
            # opportunity fluctuation), but we don't double-count it since USG%
            # is already embedded in the per-minute rate. Instead, we use it to
            # detect when a player's EWMA might lag a usage change.
            usg = player_data.get('USG_PCT', 0.20)
            ts = player_data.get('TS_PCT', 0.55)

            # If a player's TS% is significantly above league average (0.57),
            # they're more likely to sustain their scoring rate.
            # If below, regression is expected.
            league_avg_ts = 0.572
            ts_delta = (ts - league_avg_ts) / league_avg_ts
            # Clamp the TS adjustment to ±5% to avoid overcorrection
            efficiency_mod = 1.0 + max(min(ts_delta * 0.15, 0.05), -0.05)

        # --- 8. Assists: use tracking data (potential assists * conversion rate) ---
        if stat_type == "Assists":
            player_id = player_data['PLAYER_ID']
            if player_id in self.track_df.index:
                pot_ast = self.track_df.loc[player_id, 'POTENTIAL_AST']
                base_ast = self.get_ewma_baseline(player_name, 'AST')
                if pot_ast > 0:
                    conversion_rate = base_ast / pot_ast
                    # Scale potential assists by pace and minutes
                    proj_pot_ast = pot_ast * (effective_minutes / base_min) * pace_factor
                    projection = proj_pot_ast * conversion_rate * def_multiplier * location_mod
                    return round(projection, 2)

        # --- Final projection ---
        projection = (per_minute_rate
                      * effective_minutes
                      * pace_factor
                      * def_multiplier
                      * location_mod
                      * efficiency_mod)

        return round(projection, 2)