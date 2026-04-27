# src/main.py
"""
SHARP EDGE V2.1 - The Orchestrator (Full Rebuild)

Routes raw data through:
  Extractors → DeterministicProjector → NegBin/Poisson → Strategy Filter → Veto Layer → Services

Key changes from V2.0:
  1. EWMA baselines fed into projector (not raw season averages)
  2. Defensive matchup multipliers re-integrated
  3. Strategy filter blocks proven money-losing stat/direction combos
  4. Per-player projection snapshots (no more shared mutable state bug)
  5. Vetoed plays are EXCLUDED from DB logging
  6. Deduplication: won't re-alert or re-log identical plays
  7. Edge capped at 15% to prevent mirage signals
  8. Negative Binomial probability for scoring props
"""
import os
import pandas as pd
import time
from dotenv import load_dotenv

from extractors.pp_extractors import fetch_live_board
from extractors.nba_extractors import (
    get_advanced_player_baselines,
    get_team_pace_and_defense,
    get_tracking_data,
    get_opponent_matchup_multipliers,
    get_league_gamelog_for_ewma,
)
from engine.projections import DeterministicProjector
from engine.probability import calculate_probabilities, get_true_edge, calculate_confidence_score
from engine.strategy import evaluate_play, get_strategy_summary
from engine.veto import MLVetoLayer

from services.db import init_db, log_predictions, filter_new_plays
from services.notifier import send_discord_alert
from utils.utils import logger, timer

# For the Veto Layer game log lookups
from nba_api.stats.endpoints import playergamelog
from services.grader import grade_pending_bets

load_dotenv()

# PrizePicks uses non-standard abbreviations — map to official NBA ones
PP_TO_NBA_ABBR = {
    "SA": "SAS", "NY": "NYK", "GS": "GSW",
    "NO": "NOP", "UTAH": "UTA", "WSH": "WAS",
}


@timer
def run_v2_pipeline(edge_threshold=2.5):
    logger.info("=== Booting Sharp Edge V2.1 (Deterministic Engine + Strategy Filter) ===\n")

    # =========================================================================
    # 1. EXTRACTION — Fetch all data upfront (stateless)
    # =========================================================================
    logger.info("[*] Phase 1: Extracting raw data from stateless sources...")

    pp_board = fetch_live_board()
    if pp_board.empty:
        logger.info("[-] PrizePicks board is empty or failed to load. Exiting.")
        return

    # Season-long baseline (stable anchor)
    adv_season = get_advanced_player_baselines(last_n_games=0, season_type="Regular Season")
    time.sleep(0.6)

    # Recent baseline for recency detection (5-game window)
    adv_recent = get_advanced_player_baselines(last_n_games=5, season_type="Playoffs")

    if adv_season.empty or adv_recent.empty:
        logger.error("[-] NBA API timed out and failed to load player baselines. Exiting cycle.")
        return

    pace_df = get_team_pace_and_defense()
    time.sleep(0.6)

    tracking_df = get_tracking_data()
    time.sleep(0.6)

    # NEW: Fetch defensive matchup multipliers (re-integrated from V1)
    opp_multipliers = get_opponent_matchup_multipliers()
    time.sleep(0.6)

    # NEW: Fetch game logs for EWMA computation
    game_logs = get_league_gamelog_for_ewma(season_type="Playoffs")
    if game_logs.empty:
        # Fallback to regular season if playoffs hasn't started
        game_logs = get_league_gamelog_for_ewma(season_type="Regular Season")

    # =========================================================================
    # 2. INITIALIZE ENGINES
    # =========================================================================
    logger.info("[*] Phase 2: Initializing deterministic projector and veto layer...")

    projector = DeterministicProjector(
        advanced_df=adv_season,
        tracking_df=tracking_df,
        pace_df=pace_df,
        opp_multipliers=opp_multipliers,
        game_logs_df=game_logs,
    )
    veto_layer = MLVetoLayer()

    # Log strategy rules
    logger.info(get_strategy_summary())

    results = []
    vetoed_count = 0
    strategy_blocked_count = 0

    # =========================================================================
    # 3. TRANSFORM & CALCULATE — Process each prop
    # =========================================================================
    logger.info("[*] Phase 3: Processing mathematical projections for each prop...")

    for _, row in pp_board.iterrows():
        player = row['Player']
        stat = row['Stat']
        line = float(row['Line'])
        matchup = row['Matchup']

        # --- Resolve player data (per-player, no shared mutable state) ---
        season_rows = adv_season[adv_season['PLAYER_NAME'] == player]
        recent_rows = adv_recent[adv_recent['PLAYER_NAME'] == player]
        if season_rows.empty:
            continue

        true_team = season_rows.iloc[0]['TEAM_ABBREVIATION']
        season_avg_minutes = season_rows.iloc[0]['MIN']
        projected_minutes = season_avg_minutes

        # === Minute Recency Override (per-player, isolated) ===
        if not recent_rows.empty and season_avg_minutes > 0:
            recent_minutes = recent_rows.iloc[0]['MIN']

            if recent_minutes >= (season_avg_minutes * 1.20):
                projected_minutes = recent_minutes
                logger.info(f"  [!] MINUTE SPIKE: {player} (Season: {season_avg_minutes:.1f} -> Recent: {recent_minutes:.1f})")

            elif recent_minutes <= (season_avg_minutes * 0.80):
                projected_minutes = recent_minutes
                logger.info(f"  [!] MINUTE DROP: {player} (Season: {season_avg_minutes:.1f} -> Recent: {recent_minutes:.1f})")

        # --- Resolve opponent ---
        try:
            opp_team_raw = str(matchup).split(' ')[-1].upper()
            opp_team = PP_TO_NBA_ABBR.get(opp_team_raw, opp_team_raw)
        except Exception:
            continue

        # --- Determine home/away ---
        is_home = '@' not in str(matchup)

        # --- Generate projection (uses EWMA + pace + defense + TS% + blowout) ---
        projection = projector.generate_projection(
            player, opp_team, projected_minutes, stat, is_home=is_home
        )
        if projection is None or projection <= 0:
            continue

        # --- Get empirical variance for this player/stat (for NegBin calibration) ---
        stat_map = {
            "Points": "PTS", "Rebounds": "REB", "Assists": "AST",
            "Pts+Rebs+Asts": "PRA", "Pts+Rebs": "PR",
            "Pts+Asts": "PA", "Rebs+Asts": "RA",
        }
        stat_key = stat_map.get(stat)
        empirical_var = projector.get_stat_variance(player, stat_key) if stat_key else None

        # --- Calculate probability (NegBin for scoring, Poisson for counting) ---
        probs = calculate_probabilities(projection, line, stat_type=stat, empirical_variance=empirical_var)

        if projection > line:
            play = "OVER"
            implied_prob = probs["over"]
        else:
            play = "UNDER"
            implied_prob = probs["under"]

        ev_edge = get_true_edge(implied_prob, sportsbook_implied=54.2)

        # --- Strategy Filter (blocks proven money losers) ---
        should_play, strategy_reason, tier_label = evaluate_play(stat, play, ev_edge)
        if not should_play:
            strategy_blocked_count += 1
            continue

        # --- Confidence Score ---
        confidence = calculate_confidence_score(ev_edge, implied_prob, stat)

        # --- Veto Layer ---
        is_vetoed = False
        veto_reason = "CLEARED"

        try:
            player_id = season_rows.iloc[0]['PLAYER_ID']
            player_logs = playergamelog.PlayerGameLog(player_id=player_id).get_data_frames()[0]
            is_vetoed, veto_reason = veto_layer.check_veto(
                stat, matchup,
                row.get('Game Date', pd.Timestamp.now().strftime('%Y-%m-%d')),
                player_logs, play,
                ewma_projection=projection, line=line
            )
            time.sleep(0.3)
        except Exception as e:
            logger.warning(f"[!] Veto Layer check failed for {player}: {e}")

        # --- CRITICAL FIX: Do NOT add vetoed plays to results ---
        if is_vetoed:
            vetoed_count += 1
            continue

        results.append({
            "Player": player,
            "Team": true_team,
            "Matchup": matchup,
            "Stat": stat,
            "PP Line": line,
            "V2 Proj": projection,
            "Play": play,
            "Poisson Prob": implied_prob,
            "EV Edge": ev_edge,
            "Confidence": confidence,
            "Tier": tier_label,
            "Vetoed": False,  # Only non-vetoed plays reach here
            "ML Prob": "-",
            "Game Date": row.get('Game Date', None)
        })

    # =========================================================================
    # 4. LOAD & NOTIFY
    # =========================================================================
    logger.info(f"[*] Phase 4: Processing results...")
    logger.info(f"    Strategy blocked: {strategy_blocked_count} plays")
    logger.info(f"    Veto blocked: {vetoed_count} plays")

    results_df = pd.DataFrame(results)
    if not results_df.empty:
        # Sort by confidence score (not just edge size)
        results_df = results_df.sort_values(by='Confidence', ascending=False)

        # Deduplication: Filter out plays already in the DB
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
        logger.info("[-] Market is tight. Zero plays cleared all filters.")


if __name__ == "__main__":
    init_db()
    # run_v2_pipeline(edge_threshold=2.5)
    logger.info("[*] Phase 5: Grading pending bets...")
    grade_pending_bets()