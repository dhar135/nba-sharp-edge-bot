import pandas as pd
from venues import get_venue
from venues.base import VenueAdapter


def test_registry_returns_prizepicks_adapter():
    v = get_venue("prizepicks")
    assert isinstance(v, VenueAdapter)
    assert v.name == "prizepicks"


def test_unknown_venue_raises():
    import pytest
    with pytest.raises(KeyError):
        get_venue("bovada")


def test_prizepicks_board_columns(monkeypatch):
    import venues.prizepicks as pp
    fake = pd.DataFrame([{"Player": "A", "Stat": "Points", "Line": 20.5,
                          "Matchup": "BOS vs NYK", "Game Date": "2026-11-01"}])
    monkeypatch.setattr(pp, "fetch_live_board", lambda: fake)
    board = get_venue("prizepicks").fetch_board("NBA")
    assert list(board.columns) >= ["Player", "Stat", "Line", "Matchup", "Game Date"]
