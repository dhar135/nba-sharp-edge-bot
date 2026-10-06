from datetime import date


def get_current_season(d=None):
    """NBA season string for a date, e.g. "2026-27" for Oct 2026 - Jun 2027.

    Jul-Sep (offseason) returns the season that just finished.
    """
    d = d or date.today()
    start = d.year if d.month >= 10 else d.year - 1
    return f"{start}-{str(start + 1)[-2:]}"


EARLY_SEASON_DAYS = 21  # ~first 3 weeks: samples too thin to trust with real stakes


def is_early_season(d=None):
    """True during the first EARLY_SEASON_DAYS of the regular season (from Oct 20)."""
    d = d or date.today()
    if get_season_phase(d) != "Regular Season" or d.month < 10:
        return False
    return (d - date(d.year, 10, 20)).days < EARLY_SEASON_DAYS


def get_season_phase(d=None):
    """NBA calendar phase. Approximate boundaries; play-in counts as playoffs."""
    d = d or date.today()
    m, day = d.month, d.day
    if m in (11, 12, 1, 2, 3):
        return "Regular Season"
    if m == 10:
        return "Regular Season" if day >= 20 else "Offseason"
    if m == 4:
        return "Playoffs" if day >= 15 else "Regular Season"
    if m in (5, 6):
        return "Playoffs"
    return "Offseason"  # Jul-Sep
