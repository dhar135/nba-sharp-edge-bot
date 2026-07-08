import sqlite3
from datetime import datetime
import pandas as pd


def _play_row(game_date="2026-11-01"):
    return pd.DataFrame([{
        "Player": "Test Guy", "Team": "BOS", "Matchup": "BOS vs NYK",
        "Stat": "Points", "PP Line": 20.5, "Play": "UNDER",
        "EV Edge": 9.0, "V2 Proj": 17.2, "Poisson Prob": 63.0,
        "Confidence": 70.0, "Tier": "🟢 STRONG", "Vetoed": False,
        "ML Prob": "-", "Game Date": game_date,
        "Vegas Line": None, "Vegas Diff": None, "Vegas Confirms": None,
    }])


class _FakeDatetime:
    _now = datetime(2026, 10, 31)

    @classmethod
    def now(cls):
        return cls._now


def test_same_play_across_two_run_dates_logs_once(tmp_db, monkeypatch):
    import services.db as db
    monkeypatch.setattr(db, "datetime", _FakeDatetime)

    _FakeDatetime._now = datetime(2026, 10, 31)   # run day 1
    db.log_predictions(_play_row())
    _FakeDatetime._now = datetime(2026, 11, 1)    # run day 2, same game
    db.log_predictions(_play_row())

    n = sqlite3.connect(tmp_db).execute(
        "SELECT COUNT(*) FROM predictions").fetchone()[0]
    assert n == 1


def test_filter_new_plays_drops_already_logged_play(tmp_db, monkeypatch):
    import services.db as db
    monkeypatch.setattr(db, "datetime", _FakeDatetime)

    _FakeDatetime._now = datetime(2026, 10, 31)
    db.log_predictions(_play_row())
    _FakeDatetime._now = datetime(2026, 11, 1)    # next run day
    filtered = db.filter_new_plays(_play_row())
    assert filtered.empty


def test_upgrade_updates_stale_play_direction(tmp_db, monkeypatch):
    """A play whose direction flips between run days (line move) must have its
    stored `play` and `line` updated on upgrade, not just edge/tier/vegas columns —
    otherwise the grader grades the wrong side."""
    import services.db as db
    monkeypatch.setattr(db, "datetime", _FakeDatetime)

    _FakeDatetime._now = datetime(2026, 10, 31)   # run day 1
    day1 = _play_row(game_date="2026-11-01")
    day1.loc[0, "Play"] = "UNDER"
    day1.loc[0, "EV Edge"] = 9.0
    db.log_predictions(day1)

    _FakeDatetime._now = datetime(2026, 11, 1)    # run day 2, same game
    day2 = _play_row(game_date="2026-11-01")
    day2.loc[0, "Play"] = "OVER"
    day2.loc[0, "EV Edge"] = 10.0
    day2.loc[0, "PP Line"] = 21.5
    db.log_predictions(day2)

    row = sqlite3.connect(tmp_db).execute(
        "SELECT play, line FROM predictions").fetchone()
    assert row[0] == "OVER"
    assert row[1] == 21.5
