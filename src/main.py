# src/main.py
"""
SHARP EDGE V2.0 - The Orchestrator
Routes raw data through Extractors → Engine → Services
Implements Separation of Concerns: Stateless Extractors | Pure Math | Side Effects
"""
import os
import pandas as pd
from dotenv import load_dotenv

from extractors.pp_extractors import fetch_live_board
from engine import calculate_all_edges
from services.notifier import send_discord_alert
from services.db import init_db, log_predictions, filter_new_plays
from services.grader import grade_pending_bets
from utils.utils import logger, timer

load_dotenv()

@timer
def fetch_prizepicks_board():
    """
    DEPRECATED: Use extractors.pp_extractors.fetch_live_board() instead
    This function is kept for backward compatibility during Phase 1 transition
    """
    logger.warning("[!] main.fetch_prizepicks_board() is deprecated. Use extractors.pp_extractors.fetch_live_board()")
    return fetch_live_board()

@timer
def parse_prizepicks_json(json_data):
    """
    DEPRECATED: JSON parsing is handled in extractors.pp_extractors._parse_board_json()
    This function is kept for backward compatibility during Phase 1 transition
    """
    logger.warning("[!] main.parse_prizepicks_json() is deprecated. Use extractors.pp_extractors.fetch_live_board()")
    from extractors.pp_extractors import _parse_board_json
    return _parse_board_json(json_data)

if __name__ == "__main__":
    DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")
    
    # 1. Initialize DB
    init_db()
    
    logger.info("=== Sharp Edge V2.0 MVP Initialization ===\n")
    
    # Use the new stateless extractor
    pp_data_df = fetch_live_board()
    
    if not pp_data_df.empty:
        logger.info(f"\n[+] Extracted {len(pp_data_df)} STANDARD NBA lines.")
        
        edges_df = calculate_all_edges(pp_data_df, sample_size=15, edge_threshold=15.0)
        
        logger.info("\n=== THE EDGE REPORT (>15% Discrepancies) ===")
        if edges_df.empty:
            logger.info("No massive edges found on the board right now. Market is tight.")
        else:
            edges_df['Abs Diff'] = edges_df['Diff'].abs()
            edges_df = edges_df.sort_values(by='Abs Diff', ascending=False).drop(columns=['Abs Diff'])
            logger.info(edges_df.to_string(index=False))
            
            # FILTER THE SPAM: Only keep plays we haven't seen today
            new_plays_df = filter_new_plays(edges_df)
            
            if not new_plays_df.empty:
                # Send Discord Alert
                if DISCORD_WEBHOOK:
                    send_discord_alert(new_plays_df, DISCORD_WEBHOOK)
                else:
                    logger.info("[!] Discord alert skipped. No webhook URL found.")
                    
                # 2. Log to Database
                log_predictions(new_plays_df)
                
                # 3. Grade any pending bets
                grade_pending_bets()
            else:
                logger.info("[-] No NEW edges found on this run. Discord alert skipped to prevent spam.")