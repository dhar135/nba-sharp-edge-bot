# src/engine/projections.py
import pandas as pd
from utils.utils import logger, timer

class DeterministicProjector:
    def __init__(self, advanced_df, tracking_df, pace_df):
        """
        Initializes the engine with the stateless DataFrames fetched from Phase 1.
        """
        self.adv_df = advanced_df.set_index('PLAYER_NAME') if not advanced_df.empty else pd.DataFrame()
        self.track_df = tracking_df.set_index('PLAYER_ID') if not tracking_df.empty else pd.DataFrame()
        self.pace_df = pace_df.set_index('TEAM_ABBREVIATION') if not pace_df.empty else pd.DataFrame()
        
        # Calculate League Averages for normalization
        self.league_pace = self.pace_df['PACE'].mean() if not self.pace_df.empty else 100.0

    def calculate_pace_factor(self, player_team, opp_team):
        """
        Calculates the possession modifier based on the opponent's pace.
        """
        try:
            opp_pace = self.pace_df.loc[opp_team, 'PACE']
            team_pace = self.pace_df.loc[player_team, 'PACE']
            
            # Pace Factor: If opponent plays 5% faster than average, we project 5% more possessions
            pace_factor = opp_pace / self.league_pace
            return pace_factor
        except KeyError:
            return 1.0 # Default to 1.0 if team not found

    @timer
    def generate_projection(self, player_name, opp_team, projected_minutes, stat_type):
        """
        Generates a deterministic projection based on usage, true shooting, and tracking volume.
        """
        if player_name not in self.adv_df.index:
            return None

        player_data = self.adv_df.loc[player_name]
        player_id = player_data['PLAYER_ID']
        player_team = player_data['TEAM_ABBREVIATION']
        
        # 1. Base Per-Minute Rates
        base_min = player_data['MIN']
        if base_min == 0: return 0.0
        
        # 2. Game Context Modifiers
        pace_factor = self.calculate_pace_factor(player_team, opp_team)
        volume_modifier = (projected_minutes / base_min) * pace_factor

        # 3. Spatiotemporal Tracking Enhancers
        touches = self.track_df.loc[player_id, 'TOUCHES'] if player_id in self.track_df.index else 0
        pot_ast = self.track_df.loc[player_id, 'POTENTIAL_AST'] if player_id in self.track_df.index else 0
        
        # 4. Map baselines to volume modifier
        projection = 0.0
        
        # This assumes we've merged base Box Score stats into our adv_df upstream for pure math.
        # For MVP of V2, we scale their baseline production by the volume modifier, checked by USG%
        if stat_type == "Points":
            # Real projection would use: (Touches * USG% * TS%) scaled to minutes.
            # Here we apply the contextual volume modifier to their historic baseline.
            base_pts = player_data.get('PTS', 0) # Assuming PTS is merged
            projection = base_pts * volume_modifier
            
        elif stat_type == "Assists":
            # Strip out teammate shooting variance by using Potential Assists
            base_conversion_rate = player_data.get('AST', 0) / pot_ast if pot_ast > 0 else 0.5
            expected_pot_ast = pot_ast * volume_modifier
            projection = expected_pot_ast * base_conversion_rate
            
        elif stat_type == "Rebounds":
            base_reb = player_data.get('REB', 0)
            projection = base_reb * volume_modifier
            
        return round(projection, 2)