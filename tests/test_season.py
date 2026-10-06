from datetime import date
from utils.season import get_season_phase


def test_phases():
    assert get_season_phase(date(2026, 11, 15)) == "Regular Season"
    assert get_season_phase(date(2027, 2, 1)) == "Regular Season"
    assert get_season_phase(date(2026, 5, 20)) == "Playoffs"
    assert get_season_phase(date(2026, 4, 20)) == "Playoffs"
    assert get_season_phase(date(2026, 7, 5)) == "Offseason"
    assert get_season_phase(date(2026, 10, 1)) == "Offseason"


def test_current_season_string():
    from utils.season import get_current_season
    assert get_current_season(date(2026, 10, 25)) == "2026-27"
    assert get_current_season(date(2027, 2, 1)) == "2026-27"
    assert get_current_season(date(2027, 6, 10)) == "2026-27"
    assert get_current_season(date(2026, 7, 5)) == "2025-26"


def test_league_gamelog_season_type_follows_phase(monkeypatch):
    import pandas as pd
    import nba_fetcher

    seen = {}

    class FakeLog:
        def __init__(self, **kw):
            seen.update(kw)

        def get_data_frames(self):
            return [pd.DataFrame(columns=["PLAYER_NAME", "GAME_DATE", "PTS", "REB", "AST", "BLK", "STL"])]

    monkeypatch.setattr(nba_fetcher.leaguegamelog, "LeagueGameLog", FakeLog)

    monkeypatch.setattr(nba_fetcher, "get_season_phase", lambda: "Regular Season")
    nba_fetcher.get_league_gamelog()
    assert seen["season_type_all_star"] == "Regular Season"

    monkeypatch.setattr(nba_fetcher, "get_season_phase", lambda: "Playoffs")
    nba_fetcher.get_league_gamelog()
    assert seen["season_type_all_star"] == "Playoffs"

    nba_fetcher.get_league_gamelog(season_type="Regular Season")
    assert seen["season_type_all_star"] == "Regular Season"


def test_early_season_window():
    from utils.season import is_early_season
    assert not is_early_season(date(2026, 10, 10))   # offseason
    assert is_early_season(date(2026, 10, 20))       # opening week
    assert is_early_season(date(2026, 11, 9))        # day 21
    assert not is_early_season(date(2026, 11, 10))
    assert not is_early_season(date(2027, 1, 15))
    assert not is_early_season(date(2027, 5, 1))     # playoffs


def test_paper_mode_env_overrides_auto(monkeypatch):
    import main
    monkeypatch.setattr(main, "is_early_season", lambda: True)
    monkeypatch.delenv("PAPER_MODE", raising=False)
    assert main._paper_mode() is True
    monkeypatch.setenv("PAPER_MODE", "0")
    assert main._paper_mode() is False
    monkeypatch.setattr(main, "is_early_season", lambda: False)
    monkeypatch.setenv("PAPER_MODE", "1")
    assert main._paper_mode() is True
