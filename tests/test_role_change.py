import pandas as pd
from engine.role_change import detect_role_change


def _logs(minutes, teams=None):
    n = len(minutes)
    teams = teams or ["BOS"] * n
    return pd.DataFrame({
        "PLAYER_NAME": ["Guy"] * n,
        "GAME_DATE": pd.date_range("2026-01-01", periods=n).strftime("%Y-%m-%d"),
        "MIN": minutes,
        "TEAM_ABBREVIATION": teams,
    })


def test_stable_minutes_no_flag():
    changed, _ = detect_role_change(_logs([30] * 15), "Guy")
    assert not changed


def test_minutes_spike_flags():
    changed, reason = detect_role_change(_logs([22] * 10 + [34] * 5), "Guy")
    assert changed and "minutes" in reason.lower()


def test_recent_trade_flags():
    teams = ["SAC"] * 12 + ["SAS"] * 3
    changed, reason = detect_role_change(_logs([30] * 15, teams), "Guy")
    assert changed and "trade" in reason.lower()


def test_insufficient_sample_no_flag():
    changed, _ = detect_role_change(_logs([30] * 6), "Guy")
    assert not changed
