# src/engine/probability.py
from scipy.stats import poisson
import math
from utils.utils import logger

def calculate_poisson_probabilities(projected_mean, sportsbook_line):
    """
    Converts a deterministic projection into implied probabilities for Over/Under.
    Handles half-point lines (e.g., 25.5) and integer lines (e.g., 25.0) which introduce push risks.
    
    Returns: Dict of probabilities (Over, Under, Push)
    """
    if projected_mean <= 0:
        return {"over": 0.0, "under": 100.0, "push": 0.0}

    is_half_point = (sportsbook_line % 1 != 0)
    
    if is_half_point:
        # For 25.5, Under is exactly 25 or less. Over is 26 or more.
        floor_line = math.floor(sportsbook_line)
        
        prob_under = poisson.cdf(floor_line, projected_mean)
        prob_over = 1.0 - prob_under
        prob_push = 0.0
        
    else:
        # For integer lines like 25.0, we must account for the exact push probability.
        exact_line = int(sportsbook_line)
        
        prob_push = poisson.pmf(exact_line, projected_mean)
        prob_under = poisson.cdf(exact_line - 1, projected_mean)
        prob_over = 1.0 - poisson.cdf(exact_line, projected_mean)
        
    return {
        "over": round(prob_over * 100, 2),
        "under": round(prob_under * 100, 2),
        "push": round(prob_push * 100, 2)
    }

def get_true_edge(implied_prob, sportsbook_implied=54.2):
    """
    Calculates our exact EV% over the standard DFS implied line (usually -119 / 54.2%).
    """
    edge = implied_prob - sportsbook_implied
    return round(edge, 2)