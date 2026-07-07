"""Season report from the deduped predictions DB.
Usage: python scripts/season_report.py [--out Reports/season_report.md]"""
import argparse
import os
import sqlite3
from datetime import date
from math import comb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "sharp_edge.db")

# PrizePicks payout ladders: {k_correct: multiplier}. Verify against the
# current PrizePicks app before trusting ROI absolutes — ladders change.
PAYOUTS = {
    "power2": {"legs": 2, "pays": {2: 3.0}},
    "power3": {"legs": 3, "pays": {3: 5.0}},
    "flex3":  {"legs": 3, "pays": {3: 2.25, 2: 1.25}},
    "flex5":  {"legs": 5, "pays": {5: 10.0, 4: 2.0, 3: 0.4}},
}


def compute_flex_roi(win_rate, ladder):
    """Expected return per $1 slip, assuming independent legs at win_rate."""
    cfg = PAYOUTS[ladder]
    n = cfg["legs"]
    ev = sum(
        comb(n, k) * win_rate**k * (1 - win_rate)**(n - k) * mult
        for k, mult in cfg["pays"].items()
    )
    return round(ev - 1.0, 4)


def _section(conn, title, sql):
    rows = conn.execute(sql).fetchall()
    lines = [f"\n## {title}\n"]
    for r in rows:
        lines.append("| " + " | ".join(str(c) for c in r) + " |")
    return "\n".join(lines)


def main(out_path):
    conn = sqlite3.connect(DB)
    wr_row = conn.execute(
        "SELECT COUNT(*), ROUND(100.0*SUM(status='WIN')/COUNT(*),1) "
        "FROM predictions WHERE status IN ('WIN','LOSS')").fetchone()
    n, wr = wr_row
    parts = [f"# Season Report — {date.today().isoformat()}",
             f"\n**Unique graded bets:** {n} | **Win rate:** {wr}%\n",
             "\n## ROI by slip type (at current win rate)\n"]
    for ladder in PAYOUTS:
        roi = compute_flex_roi(wr / 100.0, ladder)
        parts.append(f"- {ladder}: **{roi*100:+.1f}%** per slip")

    parts.append(_section(conn, "By month",
        "SELECT substr(game_date,1,7), COUNT(*), "
        "ROUND(100.0*SUM(status='WIN')/COUNT(*),1) FROM predictions "
        "WHERE status IN ('WIN','LOSS') GROUP BY 1 ORDER BY 1"))
    parts.append(_section(conn, "By stat + direction (n>=10)",
        "SELECT stat_type, play, COUNT(*) n, "
        "ROUND(100.0*SUM(status='WIN')/COUNT(*),1) FROM predictions "
        "WHERE status IN ('WIN','LOSS') GROUP BY 1,2 HAVING n>=10 ORDER BY 4 DESC"))
    parts.append(_section(conn, "By tier",
        "SELECT tier, COUNT(*), ROUND(100.0*SUM(status='WIN')/COUNT(*),1) "
        "FROM predictions WHERE status IN ('WIN','LOSS') GROUP BY 1 ORDER BY 3 DESC"))
    parts.append(_section(conn, "By Vegas coverage",
        "SELECT COALESCE(CAST(vegas_confirms AS TEXT),'no-line'), COUNT(*), "
        "ROUND(100.0*SUM(status='WIN')/COUNT(*),1) FROM predictions "
        "WHERE status IN ('WIN','LOSS') GROUP BY 1"))

    with open(out_path, "w") as f:
        f.write("\n".join(parts) + "\n")
    print(f"Report written: {out_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(
        ROOT, "Reports", f"season_report_{date.today().isoformat()}.md"))
    main(ap.parse_args().out)
