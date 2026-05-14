# src/engine/strategy.py
"""
V2.1 Strategy Filter — Empirical Guardrails (Recalibrated 2026-05-13)

Encodes the statistical findings from the graded predictions database.
Acts as a pre-filter BEFORE the probability engine to prevent betting on
stat/direction combos with proven negative expectation.

These guardrails are data-driven and should be recalibrated monthly.
"""
from utils.utils import logger


# ---------------------------------------------------------------------------
# Empirical Win Rates from V2.1 Database (970 graded bets as of 2026-05-13)
# ---------------------------------------------------------------------------
#
# Stat + Direction breakdown (ordered by actual V2.1 win rate):
#   Blks+Stls UNDER:     65.7%  (n=35)
#   Rebounds OVER:        64.3%  (n=14, small sample)
#   Pts+Rebs+Asts UNDER: 63.6%  (n=140)
#   Points UNDER:         63.2%  (n=136)
#   Rebounds UNDER:       62.7%  (n=83)
#   Assists UNDER:        62.3%  (n=69)
#   Pts+Asts UNDER:       62.3%  (n=130)
#   Pts+Rebs UNDER:       62.1%  (n=132)
#   Rebs+Asts UNDER:      58.0%  (n=112, regressed from 70%)
#   3-PT Made UNDER:      56.2%  (n=16, small sample)
#   Rebs+Asts OVER:       53.7%  (n=41, barely profitable)
#   Steals UNDER:         52.2%  (n=23, breakeven)
#   Turnovers UNDER:      44.4%  (n=18) ← BLOCKED
#   Blocked Shots UNDER:  41.7%  (n=12) ← BLOCKED
#   Points OVER:          BLOCKED (V2.0 data: 38.4%)
#   Pts+Asts OVER:        BLOCKED (V2.0 data: 37.8%)
#   Pts+Rebs OVER:        BLOCKED (V2.0 data: 43.2%)
#   Pts+Rebs+Asts OVER:   BLOCKED (V2.0 data: 41.9%)

STRATEGY_TIERS = {
    # --- TIER 1: ELITE (64%+ actual win rate, large sample — threshold = 2.0%) ---
    ("Rebounds", "UNDER"):       {"tier": 1, "min_edge": 2.0, "label": "🟢 ELITE"},
    ("Blks+Stls", "UNDER"):     {"tier": 1, "min_edge": 2.0, "label": "🟢 ELITE"},

    # --- TIER 2: STRONG (62-64% actual win rate — threshold = 3.0%) ---
    ("Pts+Rebs+Asts", "UNDER"): {"tier": 2, "min_edge": 3.0, "label": "🟢 STRONG"},
    ("Points", "UNDER"):         {"tier": 2, "min_edge": 3.0, "label": "🟢 STRONG"},
    ("Pts+Asts", "UNDER"):      {"tier": 2, "min_edge": 3.0, "label": "🟢 STRONG"},
    ("Assists", "UNDER"):        {"tier": 2, "min_edge": 3.0, "label": "🟢 STRONG"},
    ("Pts+Rebs", "UNDER"):      {"tier": 2, "min_edge": 3.0, "label": "🟢 STRONG"},
    ("Rebounds", "OVER"):        {"tier": 2, "min_edge": 3.0, "label": "🟢 STRONG"},

    # --- TIER 3: STANDARD (56-62% actual win rate — threshold = 5.0%) ---
    ("Rebs+Asts", "UNDER"):     {"tier": 3, "min_edge": 5.0, "label": "🟡 STANDARD"},

    # --- TIER 4: CAUTIOUS (52-56% actual win rate — threshold = 8.0%) ---
    ("Rebs+Asts", "OVER"):      {"tier": 4, "min_edge": 8.0, "label": "🟡 CAUTIOUS"},
    ("Assists", "OVER"):         {"tier": 4, "min_edge": 8.0, "label": "🟡 CAUTIOUS"},

    # --- BLOCKED: Proven money losers (<45% win rate) ---
    ("Points", "OVER"):          {"tier": 5, "min_edge": 999, "label": "🔴 BLOCKED"},
    ("Pts+Asts", "OVER"):       {"tier": 5, "min_edge": 999, "label": "🔴 BLOCKED"},
    ("Pts+Rebs+Asts", "OVER"):  {"tier": 5, "min_edge": 999, "label": "🔴 BLOCKED"},
    ("Pts+Rebs", "OVER"):       {"tier": 5, "min_edge": 999, "label": "🔴 BLOCKED"},
    ("Turnovers", "UNDER"):     {"tier": 5, "min_edge": 999, "label": "🔴 BLOCKED"},
    ("Blocked Shots", "UNDER"): {"tier": 5, "min_edge": 999, "label": "🔴 BLOCKED"},
    ("Turnovers", "OVER"):      {"tier": 5, "min_edge": 999, "label": "🔴 BLOCKED"},
}

# Default for any stat/direction combo not explicitly listed
DEFAULT_STRATEGY = {"tier": 3, "min_edge": 5.0, "label": "🟡 DEFAULT"}

# ---------------------------------------------------------------------------
# Player Blacklist — model systematically mis-projects these players
# Based on 970 graded bets. Minimum 10 bets, <30% win rate.
# Recalibrate monthly or when a player's role changes significantly.
# ---------------------------------------------------------------------------
PLAYER_BLACKLIST = {
    "Ajay Mitchell",       # 3/19  = 15.8%
    "Duncan Robinson",     # 2/10  = 20.0%
    "Jarrett Allen",       # 2/9   = 22.2%
    "Scottie Barnes",      # 4/17  = 23.5%
    "Chet Holmgren",       # 0/6   = 0.0%
}


def evaluate_play(stat_type, direction, ev_edge, player_name=None):
    """
    Evaluates whether a play should be taken based on empirical strategy rules.

    Returns:
        (should_play: bool, reason: str, tier_label: str)
    """
    # Player blacklist check
    if player_name and player_name in PLAYER_BLACKLIST:
        return False, f"BLACKLISTED: {player_name} has <30% historical win rate", "🚫 BLACKLIST"

    key = (stat_type, direction)
    strategy = STRATEGY_TIERS.get(key, DEFAULT_STRATEGY)

    tier = strategy["tier"]
    min_edge = strategy["min_edge"]
    label = strategy["label"]

    if tier == 5:
        return False, f"BLOCKED: {stat_type} {direction} has <45% historical win rate", label

    if ev_edge < min_edge:
        return False, f"Edge {ev_edge:.1f}% below tier threshold ({min_edge}%)", label

    return True, f"PASS: {label} | Edge {ev_edge:.1f}% >= {min_edge}%", label


def get_strategy_summary():
    """Returns a formatted summary of all strategy rules for logging."""
    lines = ["=== STRATEGY FILTER RULES ==="]
    for (stat, direction), config in sorted(STRATEGY_TIERS.items()):
        if config["tier"] == 5:
            lines.append(f"  {config['label']} {stat} {direction}: BLOCKED")
        else:
            lines.append(f"  {config['label']} {stat} {direction}: min edge = {config['min_edge']}%")
    if PLAYER_BLACKLIST:
        lines.append(f"  🚫 Player Blacklist: {', '.join(sorted(PLAYER_BLACKLIST))}")
    return "\n".join(lines)

