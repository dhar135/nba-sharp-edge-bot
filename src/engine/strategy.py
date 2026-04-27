# src/engine/strategy.py
"""
V2.1 Strategy Filter — Empirical Guardrails

Encodes the statistical findings from the graded predictions database.
Acts as a pre-filter BEFORE the probability engine to prevent betting on
stat/direction combos with proven negative expectation.

These guardrails are data-driven and should be recalibrated monthly.
"""
from utils.utils import logger


# ---------------------------------------------------------------------------
# Empirical Win Rates from Database (1,830+ graded bets as of 2026-04-26)
# ---------------------------------------------------------------------------
#
# Direction breakdown:
#   OVER:  511W / 679L = 42.9% (LOSING)
#   UNDER: 312W / 328L = 48.8% (near breakeven)
#
# Stat + Direction breakdown (ordered by win rate):
#   Assists UNDER:      81.0%  ← GOLD
#   Rebounds OVER:       59.0%  ← STRONG
#   Rebs+Asts OVER:     55.7%  ← GOOD
#   PRA UNDER:           50.4%  ← breakeven
#   Pts+Rebs UNDER:      48.9%  ← breakeven
#   Rebs+Asts UNDER:     48.1%  ← breakeven
#   Pts+Asts UNDER:      46.8%  ← weak
#   Points UNDER:        45.2%  ← weak
#   Rebounds UNDER:      45.2%  ← weak
#   Assists OVER:        44.2%  ← bad
#   Pts+Rebs OVER:       43.2%  ← bad
#   PRA OVER:            41.9%  ← BAD
#   Points OVER:         38.4%  ← TERRIBLE
#   Pts+Asts OVER:       37.8%  ← TERRIBLE

# Strategy tiers based on empirical performance
# Tier 1 (GREEN): Proven profitable, lower edge threshold required
# Tier 2 (YELLOW): Near breakeven, standard threshold
# Tier 3 (RED): Proven unprofitable, blocked entirely until math improves

STRATEGY_TIERS = {
    # --- TIER 1: GREEN (Lower edge threshold = 2.0%) ---
    ("Assists", "UNDER"):       {"tier": 1, "min_edge": 2.0, "label": "🟢 ELITE"},
    ("Rebounds", "OVER"):        {"tier": 1, "min_edge": 2.5, "label": "🟢 STRONG"},
    ("Rebs+Asts", "OVER"):      {"tier": 1, "min_edge": 2.5, "label": "🟢 STRONG"},

    # --- TIER 2: YELLOW (Standard edge threshold = 4.0%) ---
    ("Pts+Rebs+Asts", "UNDER"): {"tier": 2, "min_edge": 4.0, "label": "🟡 STANDARD"},
    ("Pts+Rebs", "UNDER"):      {"tier": 2, "min_edge": 4.0, "label": "🟡 STANDARD"},
    ("Rebs+Asts", "UNDER"):     {"tier": 2, "min_edge": 4.0, "label": "🟡 STANDARD"},
    ("Rebounds", "UNDER"):       {"tier": 2, "min_edge": 5.0, "label": "🟡 STANDARD"},
    ("Assists", "OVER"):         {"tier": 2, "min_edge": 5.0, "label": "🟡 CAUTIOUS"},
    ("Pts+Asts", "UNDER"):      {"tier": 2, "min_edge": 5.0, "label": "🟡 CAUTIOUS"},
    ("Points", "UNDER"):         {"tier": 2, "min_edge": 5.0, "label": "🟡 CAUTIOUS"},

    # --- TIER 3: RED (Blocked — proven money losers) ---
    ("Points", "OVER"):          {"tier": 3, "min_edge": 999, "label": "🔴 BLOCKED"},
    ("Pts+Asts", "OVER"):       {"tier": 3, "min_edge": 999, "label": "🔴 BLOCKED"},
    ("Pts+Rebs+Asts", "OVER"):  {"tier": 3, "min_edge": 999, "label": "🔴 BLOCKED"},
    ("Pts+Rebs", "OVER"):       {"tier": 3, "min_edge": 999, "label": "🔴 BLOCKED"},
}

# Default for any stat/direction combo not explicitly listed
DEFAULT_STRATEGY = {"tier": 2, "min_edge": 5.0, "label": "🟡 DEFAULT"}


def evaluate_play(stat_type, direction, ev_edge):
    """
    Evaluates whether a play should be taken based on empirical strategy rules.

    Args:
        stat_type:  PrizePicks stat type (e.g., "Points", "Assists")
        direction:  "OVER" or "UNDER"
        ev_edge:    The calculated EV edge percentage

    Returns:
        (should_play: bool, reason: str, tier_label: str)
    """
    key = (stat_type, direction)
    strategy = STRATEGY_TIERS.get(key, DEFAULT_STRATEGY)

    tier = strategy["tier"]
    min_edge = strategy["min_edge"]
    label = strategy["label"]

    if tier == 3:
        return False, f"BLOCKED: {stat_type} {direction} has <42% historical win rate", label

    if ev_edge < min_edge:
        return False, f"Edge {ev_edge:.1f}% below tier threshold ({min_edge}%)", label

    return True, f"PASS: {label} | Edge {ev_edge:.1f}% >= {min_edge}%", label


def get_strategy_summary():
    """Returns a formatted summary of all strategy rules for logging."""
    lines = ["=== STRATEGY FILTER RULES ==="]
    for (stat, direction), config in sorted(STRATEGY_TIERS.items()):
        if config["tier"] == 3:
            lines.append(f"  {config['label']} {stat} {direction}: BLOCKED")
        else:
            lines.append(f"  {config['label']} {stat} {direction}: min edge = {config['min_edge']}%")
    return "\n".join(lines)
