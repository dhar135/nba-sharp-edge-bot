# Engine: Pure math, probability, and strategy calculations
# V2.1 Deterministic volume/efficiency projections with Negative Binomial + Strategy filters

from engine.projections import DeterministicProjector
from engine.probability import (
    calculate_probabilities,
    calculate_poisson_probabilities,  # Legacy alias
    get_true_edge,
    calculate_confidence_score,
)
from engine.strategy import evaluate_play, get_strategy_summary
from engine.veto import MLVetoLayer

__all__ = [
    'DeterministicProjector',
    'calculate_probabilities',
    'calculate_poisson_probabilities',
    'get_true_edge',
    'calculate_confidence_score',
    'evaluate_play',
    'get_strategy_summary',
    'MLVetoLayer',
]
