# src/main.py
"""
SHARP EDGE V2.0 - The Orchestrator
Routes raw data through Extractors → DeterministicProjector → Poisson → MLVetoLayer → Services
Full deterministic per-possession engine with situational veto layer
"""
import os
import pandas as pd
import time
from dotenv import load_dotenv

from extractors.pp_extractors import fetch_live_board
from extractors.nba_extractors import get_advanced_player_baselines, get_team_pace_and_defense, get_tracking_data
from engine.projections import DeterministicProjector
from engine.probability import calculate_poisson_probabilities, get_true_edge
from engine.veto import MLVetoLayer

from services.db import init_db, log_predictions, filter_new_plays
from services.notifier import send_discord_alert
from utils.utils import logger, timer

# For the Veto Layer backwards compatibility
from nba_api.stats.endpoints import playergamelog
from services.grader import grade_pending_bets

load_dotenv()


@timer
def run_v2_pipeline(edge_threshold=2.5):
    logger.info("=== Booting Sharp Edge V2.0 (Deterministic Engine) ===\n")
    
    # 1. EXTRACTION
    logger.info("[*] Phase 1: Extracting raw data from stateless sources...")
    pp_board = fetch_live_board()
    if pp_board.empty:
        logger.info("[-] PrizePicks board is empty or failed to load. Exiting.")
        return

    # CRITICAL FIX: Pull both a stable season baseline and a volatile recent baseline
    adv_season = get_advanced_player_baselines(last_n_games=0, season_type="Regular Season")
    adv_recent = get_advanced_player_baselines(last_n_games=5, season_type="Playoffs")
    pace_df = get_team_pace_and_defense()
    tracking_df = get_tracking_data()

    # 2. INITIALIZE ENGINES
    logger.info("[*] Phase 2: Initializing deterministic projector and veto layer...")
    projector = DeterministicProjector(adv_season, tracking_df, pace_df)
    veto_layer = MLVetoLayer()
    
    results = []

    # 3. TRANSFORM & CALCULATE
    logger.info("[*] Phase 3: Processing mathematical projections for each prop...")
    for _, row in pp_board.iterrows():
        player = row['Player']
        stat = row['Stat']
        line = float(row['Line'])
        matchup = row['Matchup']
        
        season_rows = adv_season[adv_season['PLAYER_NAME'] == player]
        recent_rows = adv_recent[adv_recent['PLAYER_NAME'] == player]
        if season_rows.empty:
            continue
        
        # CRITICAL FIX: Pull the TRUE team from the NBA API, ignoring PrizePicks' glitches
        true_team = season_rows.iloc[0]['TEAM_ABBREVIATION']
        season_avg_minutes = season_rows.iloc[0]['MIN']
        projected_minutes = season_avg_minutes

        # === THE BI-DIRECTIONAL RECENCY OVERRIDE (Injury / Playoff Rotation Context) ===
        if not recent_rows.empty and season_avg_minutes > 0:
            recent_minutes = recent_rows.iloc[0]['MIN']
            
            # 1. THE SPIKE: Playing 20% MORE minutes recently (e.g., starter hurt)
            if recent_minutes >= (season_avg_minutes * 1.20):
                projected_minutes = recent_minutes
                logger.info(f"  [!] MINUTE SPIKE DETECTED: {player} (Season: {season_avg_minutes:.1f} -> Recent: {recent_minutes:.1f})")
                projector.adv_df = adv_recent.set_index('PLAYER_NAME')
                
            # 2. THE DROP: Playing 20% LESS minutes recently (e.g., benched for playoffs)
            elif recent_minutes <= (season_avg_minutes * 0.80):
                projected_minutes = recent_minutes
                logger.info(f"  [!] MINUTE DROP DETECTED: {player} (Season: {season_avg_minutes:.1f} -> Recent: {recent_minutes:.1f})")
                projector.adv_df = adv_recent.set_index('PLAYER_NAME')
                
            # 3. STABLE: No major rotation change detected
            else:
                projector.adv_df = adv_season.set_index('PLAYER_NAME')

        try:
            opp_team = str(matchup).split(' ')[-1].upper()
        except Exception:
            continue

        projection = projector.generate_projection(player, opp_team, projected_minutes, stat)
        if projection is None or projection <= 0:
            continue

        probs = calculate_poisson_probabilities(projection, line)
        
        if projection > line:
            play = "OVER"
            implied_prob = probs["over"]
        else:
            play = "UNDER"
            implied_prob = probs["under"]

        ev_edge = get_true_edge(implied_prob, sportsbook_implied=54.2)

        is_vetoed = False
        ml_prob_val = "-"
        
        if ev_edge >= edge_threshold:
            if stat == "Points":
                try:
                    player_id = season_rows.iloc[0]['PLAYER_ID']
                    player_logs = playergamelog.PlayerGameLog(player_id=player_id).get_data_frames()[0]
                    game_date = row.get('Game Date', pd.Timestamp.now().strftime('%Y-%m-%d'))
                    is_vetoed, ml_prob_val = veto_layer.check_veto(stat, matchup, game_date, player_logs, play)
                    time.sleep(0.3)
                except Exception as e:
                    logger.warning(f"[!] Veto Layer check failed for {player}: {e}")

            results.append({
                "Player": player,
                "Team": true_team, # <--- THIS OVERRIDES THE PRIZEPICKS GLITCH
                "Matchup": matchup,
                "Stat": stat,
                "PP Line": line,
                "V2 Proj": projection,
                "Play": play,
                "Poisson Prob": implied_prob,
                "EV Edge": ev_edge,
                "Vetoed": is_vetoed,
                "ML Prob": ml_prob_val,
                "Game Date": row.get('Game Date', None)
            })

    # 4. LOAD & NOTIFY
    logger.info("[*] Phase 4: Processing results and pushing to services...")
    results_df = pd.DataFrame(results)
    if not results_df.empty:
        results_df = results_df.sort_values(by='EV Edge', ascending=False)
        
        # Filter Spam: Only alert on new or upgraded plays
        new_plays_df = filter_new_plays(results_df)
        
        if not new_plays_df.empty:
            logger.info(f"\n[+] Found {len(new_plays_df)} *new* plays to alert.")
            webhook = os.getenv("DISCORD_WEBHOOK_URL")
            if webhook:
                send_discord_alert(new_plays_df, webhook)
                logger.info("[+] Discord batched alert sent.")
            
            log_predictions(new_plays_df)
        else:
            logger.info("[-] No *new* plays to alert on this cycle.")
    else:
        logger.info("[-] Market is tight. Zero plays met the Poisson EV threshold.")


if __name__ == "__main__":
    init_db()
    run_v2_pipeline(edge_threshold=2.5)
    logger.info("[*] Phase 5: Grading pending bets...")
    grade_pending_bets()