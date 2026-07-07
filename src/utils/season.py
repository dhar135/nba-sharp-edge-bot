from datetime import date


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
