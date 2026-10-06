"""Re-grade the NBA record against the sportsbook consensus line and calibrate grades.

Reads the predictions table read-only and prints four sections:
  1. the record as graded (against the pick'em line in `line`);
  2. the same picks re-graded against `vegas_line` (median DraftKings/FanDuel/BetMGM);
  3. calibration: our stated probability (`poisson_prob`) vs how often the play hit;
  4. hit rate by edge bucket, to place grade cutoffs where hit rates really change.

Usage: python scripts/regrade_report.py path/to/sharp_edge.db
"""
import math
import sqlite3
import sys
from collections import defaultdict

GRADED = ("WIN", "LOSS", "PUSH")


def wilson(wins: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """95% Wilson interval for a hit rate; honest at small n, unlike +/- 1.96 * se."""
    if n == 0:
        return (0.0, 0.0)
    p = wins / n
    centre = (p + z * z / (2 * n)) / (1 + z * z / n)
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
    return (centre - half, centre + half)


def line_result(play: str, actual: float, line: float) -> str:
    """WIN, LOSS or PUSH for an OVER/UNDER play graded against `line`."""
    if actual == line:
        return "PUSH"
    went_over = actual > line
    return "WIN" if (play == "OVER") == went_over else "LOSS"


def summarise(label: str, results: list[str]) -> str:
    wins = results.count("WIN")
    losses = results.count("LOSS")
    pushes = results.count("PUSH")
    decided = wins + losses
    if decided == 0:
        return f"  {label:<28} n=0"
    lo, hi = wilson(wins, decided)
    return (f"  {label:<28} {wins:>4}-{losses:<4} push {pushes:<3} "
            f"hit {100 * wins / decided:5.1f}%  (95% CI {100 * lo:4.1f}-{100 * hi:4.1f})")


def bucket(value: float, edges: list[float]) -> str:
    for lo, hi in zip(edges, edges[1:]):
        if lo <= value < hi:
            return f"{lo:g}-{hi:g}" if math.isfinite(hi) else f"{lo:g}+"
    return f"<{edges[0]:g}"


def main(path: str) -> None:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT game_date, stat_type, play, line, vegas_line, actual_result, poisson_prob, "
        "edge_percent, tier, status FROM predictions WHERE status IN (?, ?, ?)", GRADED
    ).fetchall()

    print(f"Graded picks: {len(rows)}\n")

    print("1. Record as graded (vs pick'em line)")
    print(summarise("All", [r["status"] for r in rows]))
    by_month = defaultdict(list)
    for r in rows:
        by_month[r["game_date"][:7]].append(r["status"])
    for month in sorted(by_month):
        print(summarise(month, by_month[month]))

    print("\n2. Re-graded vs consensus line (rows with vegas_line)")
    both = [r for r in rows if r["vegas_line"] is not None and r["actual_result"] is not None]
    print(f"  coverage: {len(both)} of {len(rows)} graded picks have a consensus line")
    print(summarise("Same picks vs pick'em line", [r["status"] for r in both]))
    print(summarise("Same picks vs consensus",
                    [line_result(r["play"], r["actual_result"], r["vegas_line"]) for r in both]))
    gaps = [r["vegas_line"] - r["line"] for r in both]
    if gaps:
        same = sum(1 for g in gaps if g == 0)
        friendlier = sum(1 for r, g in zip(both, gaps)
                         if (r["play"] == "OVER" and g > 0) or (r["play"] == "UNDER" and g < 0))
        print(f"  lines equal: {same}; consensus harder for our side: {friendlier}; "
              f"easier: {len(gaps) - same - friendlier}")

    print("\n3. Calibration: stated probability vs hit rate (pushes excluded)")
    bins = defaultdict(list)
    for r in rows:
        if r["poisson_prob"] is None or r["status"] == "PUSH":
            continue
        bins[bucket(r["poisson_prob"], [50, 55, 60, 65, 70, math.inf])].append(r)
    for name in sorted(bins, key=lambda b: float(b.strip("<+").split("-")[0])):
        group = bins[name]
        stated = sum(r["poisson_prob"] for r in group) / len(group)
        wins = sum(1 for r in group if r["status"] == "WIN")
        lo, hi = wilson(wins, len(group))
        print(f"  {name + '%':<10} n={len(group):<5} said {stated:5.1f}%  hit "
              f"{100 * wins / len(group):5.1f}%  (95% CI {100 * lo:4.1f}-{100 * hi:4.1f})")

    print("\n4. Hit rate by edge (edge_percent = stated prob - 54.2 pick'em breakeven)")
    edges = defaultdict(list)
    for r in rows:
        if r["edge_percent"] is not None:
            edges[bucket(r["edge_percent"], [0, 2, 4, 6, 8, math.inf])].append(r["status"])
    for name in sorted(edges, key=lambda b: float(b.strip("<+").split("-")[0])):
        print(summarise(f"edge {name}%", edges[name]))

    print("\n   By tier")
    tiers = defaultdict(list)
    for r in rows:
        tiers[r["tier"] or "(none)"].append(r["status"])
    for name in sorted(tiers):
        print(summarise(name, tiers[name]))


if __name__ == "__main__":
    main(sys.argv[1])
