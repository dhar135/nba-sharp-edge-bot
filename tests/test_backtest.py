import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from backtest import backtest


def test_backtest_filters_and_scores():
    rows = [
        ("Rebounds", "UNDER", 10.0, "A", None, "WIN"),
        ("Rebounds", "UNDER", 4.0, "B", None, "WIN"),   # below 8% floor
        ("Points", "UNDER", 12.0, "C", None, "LOSS"),
        ("Points", "UNDER", 9.0, "D", False, "LOSS"),   # vegas diverges
    ]
    def evaluator(stat, play, edge, player, vegas):
        from engine.strategy import evaluate_play
        return evaluate_play(stat, play, edge, player_name=player,
                             vegas_confirms=vegas)[0]
    result = backtest(rows, evaluator)
    assert result["n"] == 2          # B and D filtered out
    assert result["wins"] == 1
    assert result["win_rate"] == 50.0
