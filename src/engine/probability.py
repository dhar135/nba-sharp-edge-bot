# src/engine/probability.py
"""
V2.1 Probability Engine — Full Rebuild

Key changes from V2.0:
  1. Negative Binomial for scoring props (handles overdispersion)
  2. Poisson kept for low-count discrete stats (assists, rebounds, blocks, steals)
  3. Empirical variance calibration from game logs
  4. Max edge cap at 15% to prevent "mirage" signals
  5. Kelly criterion-inspired confidence scaling
"""
from scipy.stats import poisson, nbinom
import math
import numpy as np
from utils.utils import logger


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Maximum allowed edge — anything above this is likely a model error, not a real edge
MAX_EDGE_CAP = 15.0

# Stats that use Negative Binomial (overdispersed scoring distributions)
NEGBIN_STATS = {"Points", "Pts+Asts", "Pts+Rebs", "Pts+Rebs+Asts"}

# Stats that use standard Poisson (low-count discrete events)
POISSON_STATS = {"Assists", "Rebounds", "Rebs+Asts", "3-PT Made",
                 "Blocked Shots", "Steals", "Turnovers", "Blks+Stls"}

# Default overdispersion ratios by stat type (variance / mean)
# Estimated from NBA player prop distributions
DEFAULT_DISPERSION = {
    "Points": 1.8,       # Scoring is highly overdispersed
    "Pts+Asts": 1.6,
    "Pts+Rebs": 1.5,
    "Pts+Rebs+Asts": 1.7,
    "Rebounds": 1.2,      # Slightly overdispersed
    "Assists": 1.15,
    "Rebs+Asts": 1.25,
    "3-PT Made": 1.3,
    "Blocked Shots": 1.1,
    "Steals": 1.1,
    "Turnovers": 1.05,
    "Blks+Stls": 1.15,
}


def _estimate_negbin_params(projected_mean, variance=None, stat_type="Points"):
    """
    Estimate Negative Binomial r and p parameters from mean and variance.

    The Negative Binomial is parameterized as:
        mean = r * (1-p) / p
        variance = r * (1-p) / p^2

    So:
        p = mean / variance
        r = mean^2 / (variance - mean)

    If variance <= mean, we fall back to Poisson (no overdispersion detected).

    Returns:
        (r, p) tuple for scipy.stats.nbinom, or None if Poisson is more appropriate
    """
    if projected_mean <= 0:
        return None

    if variance is None or variance <= 0:
        # Use default dispersion ratio
        dispersion = DEFAULT_DISPERSION.get(stat_type, 1.5)
        variance = projected_mean * dispersion

    # If variance <= mean, there's no overdispersion — use Poisson
    if variance <= projected_mean:
        return None

    # Negative Binomial parameters
    p = projected_mean / variance
    r = (projected_mean ** 2) / (variance - projected_mean)

    # Sanity bounds
    p = max(min(p, 0.999), 0.001)
    r = max(r, 0.1)

    return (r, p)


def calculate_probabilities(projected_mean, sportsbook_line, stat_type="Points",
                            empirical_variance=None):
    """
    Converts a deterministic projection into implied probabilities for Over/Under.

    Uses Negative Binomial for scoring props (overdispersed) and Poisson for
    counting stats (well-behaved discrete distributions).

    Args:
        projected_mean:    The algorithm's projection (e.g., 24.2 points)
        sportsbook_line:   The PrizePicks line (e.g., 25.5)
        stat_type:         The stat category for distribution selection
        empirical_variance: Actual variance from game logs (for NegBin calibration)

    Returns:
        Dict with 'over', 'under', 'push' probabilities (0-100 scale)
    """
    if projected_mean <= 0:
        return {"over": 0.0, "under": 100.0, "push": 0.0}

    is_half_point = (sportsbook_line % 1 != 0)

    # --- Choose distribution ---
    use_negbin = stat_type in NEGBIN_STATS
    nb_params = None

    if use_negbin:
        nb_params = _estimate_negbin_params(projected_mean, empirical_variance, stat_type)

    if nb_params is not None:
        r, p = nb_params
        # scipy's nbinom: X ~ NegBin(n=r, p=p) where p is SUCCESS probability
        # CDF gives P(X <= k)
        if is_half_point:
            floor_line = math.floor(sportsbook_line)
            prob_under = nbinom.cdf(floor_line, r, p)
            prob_over = 1.0 - prob_under
            prob_push = 0.0
        else:
            exact_line = int(sportsbook_line)
            prob_push = nbinom.pmf(exact_line, r, p)
            prob_under = nbinom.cdf(exact_line - 1, r, p)
            prob_over = 1.0 - nbinom.cdf(exact_line, r, p)
    else:
        # Standard Poisson for counting stats
        if is_half_point:
            floor_line = math.floor(sportsbook_line)
            prob_under = poisson.cdf(floor_line, projected_mean)
            prob_over = 1.0 - prob_under
            prob_push = 0.0
        else:
            exact_line = int(sportsbook_line)
            prob_push = poisson.pmf(exact_line, projected_mean)
            prob_under = poisson.cdf(exact_line - 1, projected_mean)
            prob_over = 1.0 - poisson.cdf(exact_line, projected_mean)

    return {
        "over": round(prob_over * 100, 2),
        "under": round(prob_under * 100, 2),
        "push": round(prob_push * 100, 2)
    }


# Keep old name as alias for backwards compatibility with test suite
def calculate_poisson_probabilities(projected_mean, sportsbook_line):
    """Legacy wrapper — calls the new unified probability function with Poisson."""
    return calculate_probabilities(projected_mean, sportsbook_line, stat_type="Rebounds")


def get_true_edge(implied_prob, sportsbook_implied=54.2):
    """
    Calculates our exact EV% over the standard DFS implied line (usually -119 / 54.2%).

    Now applies an edge cap to prevent false-positive "mirage" edges.
    The database showed that edges > 15% have WORSE win rates — they're model errors.
    """
    raw_edge = implied_prob - sportsbook_implied

    # Cap the edge to prevent acting on model artifacts
    capped_edge = min(raw_edge, MAX_EDGE_CAP)

    return round(capped_edge, 2)


def calculate_confidence_score(ev_edge, poisson_prob, stat_type):
    """
    Generates a 0-100 confidence score that combines edge size with
    probability quality. Used to rank plays by true quality, not just edge size.

    Empirically, moderate edges (3-8%) with high probability conviction
    outperform large edges (15%+) with uncertain probability.
    """
    # Sweet spot: edge between 3-10% is most reliable
    if 3.0 <= ev_edge <= 10.0:
        edge_quality = 1.0
    elif ev_edge < 3.0:
        edge_quality = ev_edge / 3.0  # Linear ramp up
    else:
        # Penalize edges > 10% — diminishing returns, likely model error
        edge_quality = max(0.5, 1.0 - (ev_edge - 10.0) / 20.0)

    # Probability conviction: how far from 50/50 (higher = more certain)
    prob_conviction = abs(poisson_prob - 50.0) / 50.0

    # Stat reliability weighting (from empirical DB data)
    stat_reliability = {
        "Assists": 0.95,
        "Rebounds": 0.90,
        "Rebs+Asts": 0.85,
        "Points": 0.60,
        "Pts+Rebs": 0.65,
        "Pts+Asts": 0.55,
        "Pts+Rebs+Asts": 0.65,
        "3-PT Made": 0.50,
        "Blocked Shots": 0.50,
        "Steals": 0.50,
        "Turnovers": 0.55,
        "Blks+Stls": 0.50,
    }
    reliability = stat_reliability.get(stat_type, 0.5)

    # Weighted combination
    confidence = (edge_quality * 0.35 + prob_conviction * 0.35 + reliability * 0.30) * 100

    return round(min(confidence, 100.0), 1)