import sqlite3
import pandas as pd


def _play_row_with_probs(raw_prob, poisson_prob):
    return pd.DataFrame([{
        "Player": "Prob Guy", "Team": "BOS", "Matchup": "BOS vs NYK",
        "Stat": "Points", "PP Line": 20.5, "Play": "UNDER",
        "EV Edge": 9.0, "V2 Proj": 17.2, "Poisson Prob": poisson_prob,
        "Raw Prob": raw_prob,
        "Confidence": 70.0, "Tier": "🟢 STRONG", "Vetoed": False,
        "ML Prob": "-", "Game Date": "2026-11-01",
        "Vegas Line": None, "Vegas Diff": None, "Vegas Confirms": None,
    }])


def test_raw_prob_stored_alongside_calibrated_poisson_prob(tmp_db):
    """main.py logs the CALIBRATED probability into Poisson Prob (poisson_prob column)
    but must also persist the pre-calibration Raw Prob into raw_prob, so a future
    re-fit of the calibrator trains on raw values instead of compounding itself."""
    import services.db as db

    db.log_predictions(_play_row_with_probs(raw_prob=68.0, poisson_prob=61.0))

    row = sqlite3.connect(tmp_db).execute(
        "SELECT raw_prob, poisson_prob FROM predictions").fetchone()
    assert row[0] == 68.0
    assert row[1] == 61.0
