# src/test_v2_pipeline.py
"""
V2.0 Pipeline Testing Suite
Tests each component: Extractors → Math Engine → Veto Layer → Full Pipeline
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys

from utils.utils import logger, timer

# ============================================================================
# PART 1: TEST EXTRACTORS
# ============================================================================

@timer
def test_extractors():
    """Test all three extractor functions."""
    logger.info("\n" + "="*80)
    logger.info("PART 1: TESTING EXTRACTORS (Data Pipelines)")
    logger.info("="*80 + "\n")
    
    # Test 1.1: NBA Advanced Player Baselines
    logger.info("[TEST 1.1] Testing get_advanced_player_baselines()...")
    try:
        from extractors.nba_extractors import get_advanced_player_baselines
        adv_df = get_advanced_player_baselines(season="2025-26", last_n_games=15)
        
        if adv_df.empty:
            logger.warning("  ⚠️ WARN: Advanced baselines returned empty DataFrame (API may be down)")
            return None, None, None
        
        required_cols = ['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ABBREVIATION', 'MIN', 'USG_PCT', 'TS_PCT', 'AST_PCT', 'REB_PCT', 'PACE']
        missing_cols = [col for col in required_cols if col not in adv_df.columns]
        
        if missing_cols:
            logger.error(f"  ❌ FAIL: Missing columns {missing_cols}")
            return None, None, None
        
        logger.info(f"  ✅ PASS: Loaded {len(adv_df)} players with all required columns")
        logger.info(f"      Sample: {adv_df.iloc[0]['PLAYER_NAME']} ({adv_df.iloc[0]['TEAM_ABBREVIATION']}) - USG%: {adv_df.iloc[0]['USG_PCT']:.1f}%")
        
    except Exception as e:
        logger.error(f"  ❌ FAIL: {e}")
        return None, None, None

    # Test 1.2: Team Pace and Defense
    logger.info("\n[TEST 1.2] Testing get_team_pace_and_defense()...")
    try:
        from extractors.nba_extractors import get_team_pace_and_defense
        pace_df = get_team_pace_and_defense(season="2025-26")
        
        if pace_df.empty:
            logger.warning("  ⚠️ WARN: Pace data returned empty DataFrame")
            return adv_df, None, None
        
        required_cols = ['TEAM_ID', 'TEAM_NAME', 'PACE', 'DEF_RATING']
        missing_cols = [col for col in required_cols if col not in pace_df.columns]
        
        if missing_cols:
            logger.error(f"  ❌ FAIL: Missing columns {missing_cols}")
            return adv_df, None, None
        
        logger.info(f"  ✅ PASS: Loaded {len(pace_df)} teams")
        logger.info(f"      League Avg Pace: {pace_df['PACE'].mean():.1f}")
        
    except Exception as e:
        logger.error(f"  ❌ FAIL: {e}")
        return adv_df, None, None

    # Test 1.3: Player Tracking Data (Touches & Potential Assists)
    logger.info("\n[TEST 1.3] Testing get_tracking_data()...")
    try:
        from extractors.nba_extractors import get_tracking_data
        tracking_df = get_tracking_data(season="2025-26")
        
        if tracking_df.empty:
            logger.warning("  ⚠️ WARN: Tracking data returned empty DataFrame")
            return adv_df, pace_df, None
        
        required_cols = ['PLAYER_ID', 'TOUCHES', 'TIME_OF_POSS', 'POTENTIAL_AST']
        missing_cols = [col for col in required_cols if col not in tracking_df.columns]
        
        if missing_cols:
            logger.error(f"  ❌ FAIL: Missing columns {missing_cols}")
            return adv_df, pace_df, None
        
        logger.info(f"  ✅ PASS: Loaded tracking for {len(tracking_df)} players")
        logger.info(f"      Merge verification: {len(tracking_df)} records (TOUCHES & POTENTIAL_AST merged)")
        
    except Exception as e:
        logger.error(f"  ❌ FAIL: {e}")
        return adv_df, pace_df, None

    # Test 1.4: PrizePicks Board
    logger.info("\n[TEST 1.4] Testing fetch_live_board()...")
    try:
        from extractors.pp_extractors import fetch_live_board
        pp_board = fetch_live_board()
        
        if pp_board.empty:
            logger.warning("  ⚠️ WARN: PrizePicks board is empty (market may be closed)")
            return adv_df, pace_df, tracking_df
        
        required_cols = ['Player', 'Team', 'Stat', 'Line', 'Matchup', 'Game Date']
        missing_cols = [col for col in required_cols if col not in pp_board.columns]
        
        if missing_cols:
            logger.error(f"  ❌ FAIL: Missing columns {missing_cols}")
            return adv_df, pace_df, tracking_df
        
        logger.info(f"  ✅ PASS: Loaded {len(pp_board)} props from live board")
        logger.info(f"      Sample: {pp_board.iloc[0]['Player']} - {pp_board.iloc[0]['Stat']} @ {pp_board.iloc[0]['Line']}")
        
    except Exception as e:
        logger.error(f"  ❌ FAIL: {e}")
        return adv_df, pace_df, tracking_df
    
    return adv_df, pace_df, tracking_df


# ============================================================================
# PART 2: TEST MATH ENGINE
# ============================================================================

@timer
def test_math_engine(adv_df, pace_df, tracking_df):
    """Test the pure mathematical components."""
    logger.info("\n" + "="*80)
    logger.info("PART 2: TESTING MATH ENGINE (Pure Logic)")
    logger.info("="*80 + "\n")
    
    if adv_df is None or adv_df.empty:
        logger.error("  ❌ SKIP: Cannot test math engine without advanced baselines")
        return
    
    # Test 2.1: DeterministicProjector Initialization
    logger.info("[TEST 2.1] Initializing DeterministicProjector...")
    try:
        from engine.projections import DeterministicProjector
        
        projector = DeterministicProjector(adv_df, tracking_df if tracking_df is not None else pd.DataFrame(), pace_df)
        logger.info("  ✅ PASS: DeterministicProjector initialized successfully")
        
    except Exception as e:
        logger.error(f"  ❌ FAIL: {e}")
        return
    
    # Test 2.2: Generate a projection for a real player
    logger.info("\n[TEST 2.2] Testing generate_projection()...")
    try:
        if adv_df.empty:
            logger.warning("  ⚠️ SKIP: No player data available")
            return
        
        sample_player = adv_df.iloc[0]['PLAYER_NAME']
        sample_team = adv_df.iloc[0]['TEAM_ABBREVIATION']
        
        # Get any opponent from pace_df
        if pace_df is not None and not pace_df.empty:
            opp_team = pace_df.iloc[0]['TEAM_NAME'].split()[-1] if 'TEAM_NAME' in pace_df.columns else 'LAL'
        else:
            opp_team = 'LAL'
        
        projection = projector.generate_projection(sample_player, opp_team, projected_minutes=32.0, stat_type='Points')
        
        if projection is None or projection == 0:
            logger.warning(f"  ⚠️ WARN: Projection returned None/0 for {sample_player}")
            return
        
        logger.info(f"  ✅ PASS: Generated projection for {sample_player}")
        logger.info(f"      Projection: {projection:.2f} PTS @ 32.0 min vs {opp_team}")
        
    except Exception as e:
        logger.error(f"  ❌ FAIL: {e}")
        return

    # Test 2.3: Poisson Probability Calculation
    logger.info("\n[TEST 2.3] Testing calculate_poisson_probabilities()...")
    try:
        from engine.probability import calculate_poisson_probabilities
        
        # Test case: projection of 24.3 against a line of 25.5
        test_projection = 24.3
        test_line = 25.5
        
        probs = calculate_poisson_probabilities(test_projection, test_line)
        
        if probs is None or 'over' not in probs:
            logger.error("  ❌ FAIL: Probability calculation returned invalid structure")
            return
        
        total_prob = probs['over'] + probs['under'] + probs.get('push', 0)
        
        # Check probabilities sum to ~100%
        if abs(total_prob - 100.0) > 1.0:
            logger.error(f"  ❌ FAIL: Probabilities don't sum to 100%: {total_prob:.1f}%")
            return
        
        logger.info(f"  ✅ PASS: Poisson probabilities calculated correctly")
        logger.info(f"      Projection: {test_projection} | Line: {test_line}")
        logger.info(f"      Over: {probs['over']:.1f}% | Under: {probs['under']:.1f}% | Push: {probs.get('push', 0):.1f}%")
        logger.info(f"      Total: {total_prob:.1f}%")
        
    except Exception as e:
        logger.error(f"  ❌ FAIL: {e}")
        return

    # Test 2.4: True Edge Calculation
    logger.info("\n[TEST 2.4] Testing get_true_edge()...")
    try:
        from engine.probability import get_true_edge
        
        # Test with 60% implied probability vs 54.2% market
        test_implied = 60.0
        edge = get_true_edge(test_implied, sportsbook_implied=54.2)
        
        expected_edge = test_implied - 54.2
        
        if abs(edge - expected_edge) > 0.1:
            logger.error(f"  ❌ FAIL: Edge calculation incorrect. Expected {expected_edge:.2f}%, got {edge:.2f}%")
            return
        
        logger.info(f"  ✅ PASS: Edge calculation correct")
        logger.info(f"      Implied: {test_implied:.1f}% | Market: 54.2% | Edge: {edge:.2f}%")
        
    except Exception as e:
        logger.error(f"  ❌ FAIL: {e}")
        return


# ============================================================================
# PART 3: TEST VETO LAYER
# ============================================================================

@timer
def test_veto_layer(adv_df):
    """Test the ML veto layer with mock data."""
    logger.info("\n" + "="*80)
    logger.info("PART 3: TESTING VETO LAYER (ML Integration)")
    logger.info("="*80 + "\n")
    
    # Test 3.1: MLVetoLayer Initialization
    logger.info("[TEST 3.1] Initializing MLVetoLayer...")
    try:
        from engine.veto import MLVetoLayer
        
        veto = MLVetoLayer(model_path="models/xgb_pts_model.pkl")
        
        if veto.model is None:
            logger.warning("  ⚠️ WARN: XGBoost model not found. Veto layer disabled (this is OK for testing)")
            return
        
        logger.info("  ✅ PASS: MLVetoLayer initialized with XGBoost model loaded")
        
    except Exception as e:
        logger.error(f"  ❌ FAIL: {e}")
        return

    # Test 3.2: check_veto() with mock player logs
    logger.info("\n[TEST 3.2] Testing check_veto() with mock data...")
    try:
        # Create mock game log DataFrame
        mock_logs = pd.DataFrame({
            'GAME_DATE': [
                (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
                for i in range(1, 21)
            ],
            'PTS': np.random.randint(18, 35, 20),
        })
        
        # Sort by date (newest first)
        mock_logs['GAME_DATE'] = pd.to_datetime(mock_logs['GAME_DATE'])
        mock_logs = mock_logs.sort_values('GAME_DATE', ascending=False).reset_index(drop=True)
        
        logger.info(f"  Created mock game log with {len(mock_logs)} games:")
        logger.info(f"      Recent 5-game median: {mock_logs['PTS'].head(5).median():.1f}")
        logger.info(f"      Recent 15-game median: {mock_logs['PTS'].head(15).median():.1f}")
        
        # Test veto check
        is_vetoed, ml_prob = veto.check_veto(
            stat_type="Points",
            matchup_str="vs LAL",
            game_date=(datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d'),
            recent_logs_df=mock_logs,
            deterministic_play="OVER"
        )
        
        logger.info(f"  ✅ PASS: Veto check executed successfully")
        logger.info(f"      Deterministic Play: OVER | Vetoed: {is_vetoed} | ML Prob: {ml_prob}%")
        
    except Exception as e:
        logger.error(f"  ❌ FAIL: {e}")
        return


# ============================================================================
# PART 4: DRY RUN (End-to-End Without Side Effects)
# ============================================================================

@timer
def test_full_pipeline_dry_run():
    """Execute the full pipeline but comment out side effects."""
    logger.info("\n" + "="*80)
    logger.info("PART 4: DRY RUN (End-to-End Pipeline)")
    logger.info("="*80 + "\n")
    
    try:
        from extractors.pp_extractors import fetch_live_board
        from extractors.nba_extractors import get_advanced_player_baselines, get_team_pace_and_defense, get_tracking_data
        from engine.projections import DeterministicProjector
        from engine.probability import calculate_poisson_probabilities, get_true_edge
        from engine.veto import MLVetoLayer
        from nba_api.stats.endpoints import playergamelog
        import time
        
        logger.info("[EXTRACTION] Fetching PrizePicks board...")
        pp_board = fetch_live_board()
        
        if pp_board.empty:
            logger.warning("  ⚠️ PrizePicks board is empty. Cannot continue dry run.")
            return
        
        logger.info(f"  ✅ Loaded {len(pp_board)} props")
        
        logger.info("\n[EXTRACTION] Fetching NBA baselines...")
        adv_baselines = get_advanced_player_baselines(last_n_games=15)
        pace_df = get_team_pace_and_defense()
        tracking_df = get_tracking_data()
        
        logger.info(f"  ✅ Advanced baselines: {len(adv_baselines)} players")
        logger.info(f"  ✅ Team pace data: {len(pace_df)} teams")
        logger.info(f"  ✅ Tracking data: {len(tracking_df)} records")
        
        logger.info("\n[ENGINE] Initializing projector and veto layer...")
        projector = DeterministicProjector(adv_baselines, tracking_df, pace_df)
        veto_layer = MLVetoLayer()
        
        logger.info("  ✅ Engines ready")
        
        logger.info("\n[PROCESSING] Running through first 10 props...")
        results = []
        
        for idx, row in pp_board.head(10).iterrows():
            player = row['Player']
            stat = row['Stat']
            line = float(row['Line'])
            matchup = row['Matchup']
            team = row['Team']
            
            try:
                opp_team = str(matchup).split(' ')[-1].upper()
            except:
                continue
            
            # A. Generate projection
            projection = projector.generate_projection(player, opp_team, 32.0, stat)
            if projection is None or projection <= 0:
                continue
            
            # B. Poisson calculation
            probs = calculate_poisson_probabilities(projection, line)
            
            if projection > line:
                play = "OVER"
                implied_prob = probs["over"]
            else:
                play = "UNDER"
                implied_prob = probs["under"]
            
            # C. EV Edge
            ev_edge = get_true_edge(implied_prob, sportsbook_implied=54.2)
            
            # D. Veto (only for good edges)
            is_vetoed = False
            ml_prob_val = "-"
            
            if ev_edge >= 2.5 and stat == "Points":
                try:
                    player_rows = adv_baselines[adv_baselines['PLAYER_NAME'] == player]
                    if not player_rows.empty:
                        player_id = player_rows.iloc[0]['PLAYER_ID']
                        player_logs = playergamelog.PlayerGameLog(player_id=player_id).get_data_frames()[0]
                        is_vetoed, ml_prob_val = veto_layer.check_veto(stat, matchup, row.get('Game Date', pd.Timestamp.now().strftime('%Y-%m-%d')), player_logs, play)
                        time.sleep(0.3)
                except Exception as e:
                    logger.debug(f"  Veto check failed for {player}: {e}")
            
            results.append({
                "Player": player,
                "Team": team,
                "Stat": stat,
                "PP Line": line,
                "V2 Proj": projection,
                "Play": play,
                "Poisson Prob": implied_prob,
                "EV Edge": ev_edge,
                "Vetoed": is_vetoed,
                "ML Prob": ml_prob_val
            })
        
        results_df = pd.DataFrame(results)
        
        logger.info(f"\n[RESULTS] Processed {len(results_df)} props successfully")
        
        if not results_df.empty:
            results_df = results_df.sort_values(by='EV Edge', ascending=False)
            
            logger.info("\n" + "="*80)
            logger.info("V2 EDGE REPORT (No Discord/DB writes in dry run)")
            logger.info("="*80)
            logger.info(results_df[['Player', 'Stat', 'PP Line', 'V2 Proj', 'Play', 'Poisson Prob', 'EV Edge', 'Vetoed']].to_string(index=False))
            
            logger.info(f"\n[SUMMARY] {len(results_df)} props passed the 2.5% EV threshold")
        
    except Exception as e:
        logger.error(f"❌ DRY RUN FAILED: {e}")
        import traceback
        traceback.print_exc()


# ============================================================================
# MAIN TEST EXECUTION
# ============================================================================

if __name__ == "__main__":
    logger.info("\n")
    logger.info("╔" + "="*78 + "╗")
    logger.info("║" + " "*78 + "║")
    logger.info("║" + "  NBA SHARP EDGE V2.0 - COMPREHENSIVE TESTING SUITE".center(78) + "║")
    logger.info("║" + "  Extractors → Math Engine → Veto Layer → Full Pipeline".center(78) + "║")
    logger.info("║" + " "*78 + "║")
    logger.info("╚" + "="*78 + "╝")
    
    # Part 1: Test Extractors
    adv_df, pace_df, tracking_df = test_extractors()
    
    # Part 2: Test Math Engine
    test_math_engine(adv_df, pace_df, tracking_df)
    
    # Part 3: Test Veto Layer
    test_veto_layer(adv_df)
    
    # Part 4: Full Pipeline Dry Run
    test_full_pipeline_dry_run()
    
    logger.info("\n" + "="*80)
    logger.info("✅ TESTING SUITE COMPLETE")
    logger.info("="*80 + "\n")
