from datetime import date
from utils.season import get_season_phase


def test_phases():
    assert get_season_phase(date(2026, 11, 15)) == "Regular Season"
    assert get_season_phase(date(2027, 2, 1)) == "Regular Season"
    assert get_season_phase(date(2026, 5, 20)) == "Playoffs"
    assert get_season_phase(date(2026, 4, 20)) == "Playoffs"
    assert get_season_phase(date(2026, 7, 5)) == "Offseason"
    assert get_season_phase(date(2026, 10, 1)) == "Offseason"
