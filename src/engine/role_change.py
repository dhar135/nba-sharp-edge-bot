"""Role-change detection — replaces the reactive player blacklist.
Flags players whose recent usage context invalidates their EWMA baseline."""


def detect_role_change(game_logs_df, player_name, threshold=0.20):
    """Returns (changed, reason). Flags if last-5-game avg minutes deviate
    more than `threshold` from season avg, or the player changed teams
    within their last 5 games."""
    if game_logs_df is None or game_logs_df.empty:
        return False, ""
    logs = game_logs_df[game_logs_df["PLAYER_NAME"] == player_name]
    if len(logs) < 10:
        return False, ""
    logs = logs.sort_values("GAME_DATE", ascending=False)

    recent_min = logs.head(5)["MIN"].astype(float).mean()
    season_min = logs["MIN"].astype(float).mean()
    if season_min > 0 and abs(recent_min - season_min) / season_min > threshold:
        return True, f"ROLE CHANGE: minutes {season_min:.1f} -> {recent_min:.1f}"

    if "TEAM_ABBREVIATION" in logs.columns:
        season_team = logs["TEAM_ABBREVIATION"].mode().iloc[0]
        if (logs.head(5)["TEAM_ABBREVIATION"] != season_team).any():
            return True, f"ROLE CHANGE: recent trade off {season_team}"

    return False, ""
