# NBA Improvements + Kalshi Venue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the data-integrity bugs found in the season review, cut the unprofitable bet volume, add the missing safety signals (Vegas gate, role-change detection, injuries, calibration), and add Kalshi as a read-only second venue behind a venue abstraction.

**Architecture:** Keep the existing pipeline (extract → project → probability → strategy → veto → notify → grade). Changes are surgical: fix the dedup key in `services/db.py`, tighten `engine/strategy.py`, add small new modules (`engine/role_change.py`, `services/injuries.py`, `venues/`), and add analysis scripts under `scripts/`. All findings referenced here come from `Reports/season_review_and_roadmap_2026-07-05.md`.

**Tech Stack:** Python 3, SQLite (`sharp_edge.db`), pandas, scipy, scikit-learn (new), pytest (new), requests. Kalshi via its public REST API (read-only market data — no trading in this plan).

## Global Constraints

- Dedup identity for a bet is `(game_date, player, stat_type)` — never the run date.
- Vetoed/blocked plays are NEVER logged to the DB (existing invariant — preserve it).
- All new code lives under `src/`; analysis/one-off scripts under `scripts/`; tests under `tests/`.
- Tests must never touch the real `sharp_edge.db` — use the `tmp_db` fixture from Task 1.
- Network calls must degrade gracefully (return empty, log a warning) — the pipeline must never crash because one feed is down.
- Run tests with `python -m pytest` from the repo root.
- Kalshi and ESPN payload shapes must be verified against live/current docs at execution time (fetch docs via Context7/web before implementing Tasks 10 and 15); parsers are tested against checked-in fixtures.

---

### Task 1: Commit pending work + pytest scaffolding

The working tree has undeployed strategy changes (De'Aaron Fox blacklist, Vegas integration). Ship them, then set up the test harness every later task uses.

**Files:**
- Modify: `requirements.txt`
- Create: `pytest.ini`
- Create: `tests/__init__.py` (empty)
- Create: `tests/conftest.py`

**Interfaces:**
- Produces: `tmp_db` pytest fixture — patches `services.db.DB_NAME` to a temp file and runs `init_db()`; returns the path (str). All DB tests consume this.

- [ ] **Step 1: Commit the existing working-tree changes**

```bash
git add src/engine/strategy.py src/main.py src/services/db.py src/services/notifier.py src/services/odds_api.py
git commit -m "feat: Vegas line comparison + Fox/Vassell blacklist (was undeployed since May)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 2: Add test dependencies**

Append to `requirements.txt` (scipy/joblib are already imported by existing code but were never pinned):

```
pytest==8.*
scipy
joblib
scikit-learn
```

Run: `pip install -r requirements.txt`

- [ ] **Step 3: Create pytest config**

`pytest.ini`:

```ini
[pytest]
pythonpath = src
testpaths = tests
```

- [ ] **Step 4: Create the DB fixture**

`tests/conftest.py`:

```python
import pytest


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    """Fresh SQLite DB per test; patches services.db to use it."""
    db_path = str(tmp_path / "test.db")
    import services.db as db
    monkeypatch.setattr(db, "DB_NAME", db_path)
    db.init_db()
    return db_path
```

- [ ] **Step 5: Verify pytest collects (0 tests is fine, no errors)**

Run: `python -m pytest --collect-only`
Expected: exits 0, "no tests ran" / empty collection, no import errors.

- [ ] **Step 6: Commit**

```bash
git add pytest.ini tests/ requirements.txt
git commit -m "chore: pytest scaffolding + pin missing deps"
```

---

### Task 2: Fix the dedup key (the double-counting bug)

`log_predictions` and `filter_new_plays` key on `(date, player, stat_type, game_date)` where `date` is the run date — so the same play alerted on two run days is logged twice. 2,952 of 3,820 graded rows were duplicates. Key must be `(game_date, player, stat_type)`, with `game_date` falling back to the run date when missing.

**Files:**
- Modify: `src/services/db.py:94-98` (log_predictions dedup query), `src/services/db.py:126-131` (insert), `src/services/db.py:177-181` (filter_new_plays query)
- Test: `tests/test_db_dedup.py`

**Interfaces:**
- Consumes: `tmp_db` fixture (Task 1).
- Produces: unchanged public signatures `log_predictions(df)`, `filter_new_plays(df) -> DataFrame`; new dedup semantics relied on by Task 3's UNIQUE index.

- [ ] **Step 1: Write the failing test**

`tests/test_db_dedup.py`:

```python
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
```

- [ ] **Step 2: Run to verify both fail**

Run: `python -m pytest tests/test_db_dedup.py -v`
Expected: FAIL — first test finds 2 rows, second returns a non-empty frame (today's date differs so the old key never matches).

- [ ] **Step 3: Fix both queries**

In `log_predictions`, replace the dedup SELECT and use the resolved game date in the INSERT:

```python
        # DEDUP KEY: the bet's identity is (game_date, player, stat_type).
        # Run date must NOT be part of the key (caused season-long double logging).
        dedup_game_date = game_date or today_date
        cursor.execute('''
            SELECT id, ev_edge FROM predictions
            WHERE game_date = ? AND player = ? AND stat_type = ?
        ''', (dedup_game_date, row['Player'], row['Stat']))
```

…and in the INSERT pass `dedup_game_date` instead of `game_date`.

In `filter_new_plays`, same replacement:

```python
        dedup_game_date = game_date or today_date
        cursor.execute('''
            SELECT ev_edge, edge_percent FROM predictions
            WHERE game_date = ? AND player = ? AND stat_type = ?
        ''', (dedup_game_date, row['Player'], row['Stat']))
```

- [ ] **Step 4: Run tests to verify pass**

Run: `python -m pytest tests/test_db_dedup.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/services/db.py tests/test_db_dedup.py
git commit -m "fix: dedup on (game_date, player, stat_type) — kills season-long double logging"
```

---

### Task 3: One-time DB cleanup + UNIQUE index

Collapse the 1,195 historical duplicate groups (keep the earliest row = the play as first alerted), backfill NULL `game_date`, then enforce uniqueness at the schema level.

**Files:**
- Create: `scripts/dedupe_db.py`
- Modify: `src/services/db.py` (`init_db` — create index)

**Interfaces:**
- Consumes: Task 2's dedup semantics (index would fail to build on duplicates).
- Produces: UNIQUE index `idx_pred_unique` on `predictions(game_date, player, stat_type)`.

- [ ] **Step 1: Write the cleanup script**

`scripts/dedupe_db.py`:

```python
"""One-time cleanup: collapse duplicate predictions created by the old
run-date dedup key. Keeps the earliest row (MIN id) per bet identity.
Backs up the DB to sharp_edge.db.bak first."""
import os
import shutil
import sqlite3

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "sharp_edge.db")

shutil.copy(DB, DB + ".bak")
print(f"Backup written: {DB}.bak")

conn = sqlite3.connect(DB)
before = conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]

conn.execute("UPDATE predictions SET game_date = date WHERE game_date IS NULL")
conn.execute("""
    DELETE FROM predictions WHERE id NOT IN (
        SELECT MIN(id) FROM predictions
        GROUP BY game_date, player, stat_type
    )
""")
conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_pred_unique
                ON predictions(game_date, player, stat_type)""")
conn.commit()

after = conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
graded = conn.execute(
    "SELECT COUNT(*), ROUND(100.0*SUM(status='WIN')/COUNT(*),1) "
    "FROM predictions WHERE status IN ('WIN','LOSS')").fetchone()
print(f"Rows: {before} -> {after} (removed {before - after})")
print(f"Graded unique bets: {graded[0]} | Win rate: {graded[1]}%")
```

- [ ] **Step 2: Add index creation to `init_db`**

In `src/services/db.py:init_db`, after the column migrations:

```python
    try:
        cursor.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_pred_unique
                          ON predictions(game_date, player, stat_type)""")
    except sqlite3.OperationalError:
        pass  # legacy DB still has duplicates — run scripts/dedupe_db.py
```

- [ ] **Step 3: Run the cleanup**

Run: `python scripts/dedupe_db.py`
Expected: `Rows: 4001 -> ~2100` (2 PENDING + 38 PUSH + 141 VOID also collapse), `Graded unique bets: ~1665 | Win rate: ~60.2%`. Verify the numbers match the season review before proceeding.

- [ ] **Step 4: Verify index exists and full suite passes**

Run: `sqlite3 sharp_edge.db "PRAGMA index_list(predictions);"` → contains `idx_pred_unique` with unique=1.
Run: `python -m pytest -v` → all pass.

- [ ] **Step 5: Commit**

```bash
git add scripts/dedupe_db.py src/services/db.py
git commit -m "chore: one-time dedupe of predictions table + UNIQUE index"
```

---

### Task 4: Delete legacy V1 modules

`src/db.py`, `src/engine.py`, `src/notifier.py` are V1 leftovers. Verified: nothing imports them (`src/main.py` uses `services/*`; `services/grader.py` imports `src/nba_fetcher.py`, which stays).

**Files:**
- Delete: `src/db.py`, `src/engine.py`, `src/notifier.py`

- [ ] **Step 1: Re-verify no imports reference them**

Run: `grep -rn "^from db import\|^import db$\|^from engine import\|^from notifier import\|^import notifier" src/ --include="*.py"`
Expected: no output. (`prep_ml_data.py` and `train_model.py` must not appear; if they do, stop and keep the referenced file.)

- [ ] **Step 2: Delete and confirm the pipeline still imports**

```bash
git rm src/db.py src/engine.py src/notifier.py
python -c "import sys; sys.path.insert(0,'src'); import main"
```

Expected: no ImportError (nba_api network calls don't fire on import).

- [ ] **Step 3: Run tests, commit**

Run: `python -m pytest -v` → all pass.

```bash
git commit -m "chore: remove dead V1 modules (db, engine, notifier)"
```

---

### Task 5: Season report script (deduped WR + payout-aware ROI)

Replace hand-run SQL with a reusable report generator, including ROI under real PrizePicks payout ladders instead of the fictional 52.4% breakeven.

**Files:**
- Create: `scripts/season_report.py`
- Test: `tests/test_report_metrics.py`

**Interfaces:**
- Produces: `compute_flex_roi(win_rate: float, ladder: str) -> float` (expected multiplier minus 1, e.g. -0.03 = -3% ROI) and `PAYOUTS` dict — reused by Task 13 (backtest).

- [ ] **Step 1: Write the failing test**

`tests/test_report_metrics.py`:

```python
from math import comb, isclose
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from season_report import compute_flex_roi


def test_power2_breakeven_is_577():
    # 2-pick power pays 3x: breakeven per-leg WR = sqrt(1/3) ≈ 0.5774
    assert compute_flex_roi(0.5774, "power2") == 0.0 or \
        isclose(compute_flex_roi(0.5774, "power2"), 0.0, abs_tol=1e-3)


def test_flex5_roi_at_60_pct_is_positive():
    assert compute_flex_roi(0.60, "flex5") > 0.0


def test_flex5_roi_at_52_pct_is_negative():
    assert compute_flex_roi(0.52, "flex5") < 0.0
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_report_metrics.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement the script**

`scripts/season_report.py`:

```python
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
```

- [ ] **Step 4: Run tests + generate the report**

Run: `python -m pytest tests/test_report_metrics.py -v` → 3 passed.
Run: `python scripts/season_report.py` → report file appears under `Reports/`. Sanity-check WR ≈ 60.2%.

- [ ] **Step 5: Commit**

```bash
git add scripts/season_report.py tests/test_report_metrics.py Reports/
git commit -m "feat: deduped season report with payout-aware ROI"
```

---

### Task 6: Global 8% edge floor

Bets with 2.5–8% edge ran at 50.5–52.2% (losing money). Raise every non-blocked tier's `min_edge` to at least 8.0.

**Files:**
- Modify: `src/engine/strategy.py:38-69`
- Test: `tests/test_strategy.py`

**Interfaces:**
- Produces: `evaluate_play(stat_type, direction, ev_edge, player_name=None)` unchanged signature; new behavior: nothing below 8.0 edge passes.

- [ ] **Step 1: Write the failing test**

`tests/test_strategy.py`:

```python
from engine.strategy import evaluate_play, STRATEGY_TIERS, DEFAULT_STRATEGY


def test_no_tier_admits_below_8_pct():
    floors = [c["min_edge"] for c in STRATEGY_TIERS.values() if c["tier"] != 5]
    floors.append(DEFAULT_STRATEGY["min_edge"])
    assert min(floors) >= 8.0


def test_seven_point_nine_edge_is_rejected():
    ok, reason, _ = evaluate_play("Rebounds", "UNDER", 7.9)
    assert not ok


def test_eight_pct_edge_passes_on_open_combo():
    ok, reason, _ = evaluate_play("Rebounds", "UNDER", 8.0)
    assert ok


def test_blocked_combos_stay_blocked():
    ok, _, _ = evaluate_play("Points", "OVER", 14.0)
    assert not ok
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_strategy.py -v`
Expected: first two tests FAIL (ELITE floor is 2.0).

- [ ] **Step 3: Raise the floors**

In `STRATEGY_TIERS`, set `min_edge: 8.0` for every tier-1 through tier-4 entry, and `DEFAULT_STRATEGY = {"tier": 3, "min_edge": 8.0, "label": "🟡 DEFAULT"}`. Keep tier numbers and labels (they still encode historical WR for reporting). Update the comment header to note: "2026-07 recalibration: global 8% floor — 2.5–8% edge bucket ran at 50.5–52.2% over 745 deduped bets."

- [ ] **Step 4: Run tests**

Run: `python -m pytest tests/test_strategy.py -v` → 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/engine/strategy.py tests/test_strategy.py
git commit -m "feat: global 8% edge floor — removes breakeven volume"
```

---

### Task 7: Vegas divergence hard block

Vegas-diverging picks ran 37.5% (deduped). Make divergence a block inside `evaluate_play`, and reorder `main.py` so the Vegas comparison happens *before* the strategy filter.

**Files:**
- Modify: `src/engine/strategy.py` (evaluate_play signature), `src/main.py:193-231` (reorder: Vegas lookup before strategy filter, pass result in)
- Test: `tests/test_strategy.py` (extend)

**Interfaces:**
- Produces: `evaluate_play(stat_type, direction, ev_edge, player_name=None, vegas_confirms=None)` — `False` blocks, `True`/`None` pass through. Task 8 of the NFL plan reuses this signature.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_strategy.py`:

```python
def test_vegas_divergence_blocks():
    ok, reason, label = evaluate_play("Rebounds", "UNDER", 10.0, vegas_confirms=False)
    assert not ok and "VEGAS" in reason


def test_vegas_confirm_and_no_data_pass():
    assert evaluate_play("Rebounds", "UNDER", 10.0, vegas_confirms=True)[0]
    assert evaluate_play("Rebounds", "UNDER", 10.0, vegas_confirms=None)[0]
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_strategy.py -v`
Expected: TypeError (unexpected keyword) → FAIL.

- [ ] **Step 3: Implement**

In `evaluate_play`, add the parameter and insert after the blacklist check:

```python
    # Vegas gate: deduped season data shows diverging picks ran 37.5% (n=16).
    # Divergence is a hard block; no-line props are the profitable niche.
    if vegas_confirms is False:
        return False, "VEGAS DIVERGES: major-book line contradicts play direction", "🏦 VEGAS-BLOCK"
```

In `src/main.py`: move the `get_vegas_comparison(...)` call (currently after the veto layer) to immediately after `play`/`implied_prob` are determined and *before* `evaluate_play`, then pass `vegas_confirms=vegas_confirms` into `evaluate_play`. Add a `vegas_blocked_count` counter logged in Phase 4 alongside the other counters.

- [ ] **Step 4: Run full suite**

Run: `python -m pytest -v` → all pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/strategy.py src/main.py tests/test_strategy.py
git commit -m "feat: Vegas divergence is a hard block, checked before strategy filter"
```

---

### Task 8: Season-phase detection + playoff shutdown

June (playoffs) ran 52.0% — unprofitable. `main.py` also hardcodes `season_type="Playoffs"`, which will crash the pipeline next October. Detect the phase from the calendar; skip playoff slates unless explicitly enabled.

**Files:**
- Create: `src/utils/season.py`
- Modify: `src/main.py:69-93` (use detected phase; early-exit guard)
- Test: `tests/test_season.py`

**Interfaces:**
- Produces: `get_season_phase(d: datetime.date | None = None) -> str` returning `"Regular Season" | "Playoffs" | "Offseason"` (strings match nba_api's `season_type` values). Reused by the NFL plan's scheduling.

- [ ] **Step 1: Write the failing test**

`tests/test_season.py`:

```python
from datetime import date
from utils.season import get_season_phase


def test_phases():
    assert get_season_phase(date(2026, 11, 15)) == "Regular Season"
    assert get_season_phase(date(2027, 2, 1)) == "Regular Season"
    assert get_season_phase(date(2026, 5, 20)) == "Playoffs"
    assert get_season_phase(date(2026, 4, 20)) == "Playoffs"
    assert get_season_phase(date(2026, 7, 5)) == "Offseason"
    assert get_season_phase(date(2026, 10, 1)) == "Offseason"
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_season.py -v` → FAIL (module missing).

- [ ] **Step 3: Implement**

`src/utils/season.py`:

```python
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
```

In `src/main.py` top of `run_v2_pipeline`:

```python
    from utils.season import get_season_phase
    phase = get_season_phase()
    if phase == "Offseason":
        logger.info("[-] Offseason — no slate. Exiting.")
        return
    if phase == "Playoffs" and os.getenv("RUN_DURING_PLAYOFFS", "0") != "1":
        logger.info("[-] Playoffs: engine unprofitable in this regime (June 2026: 52.0%). "
                    "Set RUN_DURING_PLAYOFFS=1 to override. Exiting.")
        return
```

Replace the hardcoded strings: `get_advanced_player_baselines(last_n_games=5, season_type=phase)` and `get_league_gamelog_for_ewma(season_type=phase)` (drop the manual playoff→regular fallback; the phase is now correct by construction).

- [ ] **Step 4: Run full suite**

Run: `python -m pytest -v` → all pass.

- [ ] **Step 5: Commit**

```bash
git add src/utils/season.py src/main.py tests/test_season.py
git commit -m "feat: season-phase detection + playoff shutdown guard"
```

---

### Task 9: Role-change detector replaces the player blacklist

Name lists are reactive and overfit (Robinson went 5/5 *after* blacklisting; Fox's 0/25 was a mid-season role change the model missed). Detect the cause instead: recent minutes deviating >20% from season baseline, or a recent trade.

**Files:**
- Create: `src/engine/role_change.py`
- Modify: `src/engine/strategy.py` (empty `PLAYER_BLACKLIST`), `src/main.py` (call detector before projection)
- Test: `tests/test_role_change.py`

**Interfaces:**
- Consumes: `game_logs_df` (LeagueGameLog frame with `PLAYER_NAME`, `GAME_DATE`, `MIN`, `TEAM_ABBREVIATION`) already fetched in `main.py`.
- Produces: `detect_role_change(game_logs_df, player_name, threshold=0.20) -> tuple[bool, str]`.

- [ ] **Step 1: Write the failing tests**

`tests/test_role_change.py`:

```python
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
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_role_change.py -v` → FAIL (module missing).

- [ ] **Step 3: Implement**

`src/engine/role_change.py`:

```python
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
```

In `src/main.py`, inside the prop loop after resolving `season_rows` (before generating the projection):

```python
        role_changed, role_reason = detect_role_change(game_logs, player)
        if role_changed:
            logger.info(f"  [X] SKIP {player}: {role_reason}")
            strategy_blocked_count += 1
            continue
```

(import `from engine.role_change import detect_role_change` at the top). In `src/engine/strategy.py`, set `PLAYER_BLACKLIST = set()` and leave a comment: "Replaced by engine/role_change.py — name lists were reactive and overfit."

- [ ] **Step 4: Run full suite**

Run: `python -m pytest -v` → all pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/role_change.py src/engine/strategy.py src/main.py tests/test_role_change.py
git commit -m "feat: role-change detector replaces player blacklist"
```

---

### Task 10: Injury feed (kill the 141 DNP voids)

Skip players listed Out/Doubtful on ESPN's public NBA injuries endpoint. **At execution: fetch a live sample of the endpoint first and adjust the parser to the actual payload shape; the fixture below encodes the expected shape.**

**Files:**
- Create: `src/services/injuries.py`
- Create: `tests/fixtures/espn_injuries.json`
- Modify: `src/main.py` (skip injured players in the prop loop)
- Test: `tests/test_injuries.py`

**Interfaces:**
- Produces: `fetch_injury_blocklist() -> set[str]` (lowercased player names; empty set on any failure) and `parse_injuries(payload: dict) -> set[str]`.

- [ ] **Step 1: Create the fixture and failing test**

`tests/fixtures/espn_injuries.json`:

```json
{
  "injuries": [
    {
      "displayName": "Boston Celtics",
      "injuries": [
        {"status": "Out", "athlete": {"displayName": "Star Player"}},
        {"status": "Day-To-Day", "athlete": {"displayName": "Healthy Enough Guy"}},
        {"status": "Doubtful", "athlete": {"displayName": "Hurt Guy"}}
      ]
    }
  ]
}
```

`tests/test_injuries.py`:

```python
import json
import os
from services.injuries import parse_injuries

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "espn_injuries.json")


def test_out_and_doubtful_blocked_day_to_day_not():
    payload = json.load(open(FIXTURE))
    blocked = parse_injuries(payload)
    assert "star player" in blocked
    assert "hurt guy" in blocked
    assert "healthy enough guy" not in blocked


def test_malformed_payload_returns_empty():
    assert parse_injuries({}) == set()
    assert parse_injuries({"injuries": [{"nope": 1}]}) == set()
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_injuries.py -v` → FAIL (module missing).

- [ ] **Step 3: Implement**

`src/services/injuries.py`:

```python
"""ESPN public injuries feed — blocks Out/Doubtful players before projection.
Payload shape verified against the live endpoint on implementation day."""
import requests
from utils.utils import logger

ESPN_INJURIES_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"
BLOCKING_STATUSES = {"out", "doubtful"}


def parse_injuries(payload):
    blocked = set()
    for team in payload.get("injuries", []) or []:
        for inj in team.get("injuries", []) or []:
            status = str(inj.get("status", "")).lower()
            name = (inj.get("athlete") or {}).get("displayName", "")
            if status in BLOCKING_STATUSES and name:
                blocked.add(name.lower().strip())
    return blocked


def fetch_injury_blocklist():
    try:
        resp = requests.get(ESPN_INJURIES_URL, timeout=10)
        if resp.status_code != 200:
            logger.warning(f"[!] Injury feed HTTP {resp.status_code} — continuing without it.")
            return set()
        blocked = parse_injuries(resp.json())
        logger.info(f"[+] Injury feed: {len(blocked)} players Out/Doubtful.")
        return blocked
    except Exception as e:
        logger.warning(f"[!] Injury feed failed ({e}) — continuing without it.")
        return set()
```

In `src/main.py` Phase 1, add `injury_blocklist = fetch_injury_blocklist()`; in the prop loop (before projection):

```python
        if player.lower().strip() in injury_blocklist:
            logger.info(f"  [X] SKIP {player}: listed Out/Doubtful")
            strategy_blocked_count += 1
            continue
```

- [ ] **Step 4: Run full suite + live smoke test**

Run: `python -m pytest -v` → all pass.
Run: `python -c "import sys; sys.path.insert(0,'src'); from services.injuries import fetch_injury_blocklist; print(len(fetch_injury_blocklist()))"` → prints a number ≥ 0, no traceback. (July: likely 0 — verify again in October.)

- [ ] **Step 5: Commit**

```bash
git add src/services/injuries.py src/main.py tests/
git commit -m "feat: ESPN injury feed blocks Out/Doubtful players"
```

---

### Task 11: Closing-line capture (CLV tracking)

CLV is the fastest ground truth for whether an edge is real. A second cron run near first lock re-fetches the board and stamps each pending prediction with the closing line.

**Files:**
- Create: `scripts/capture_closing_lines.py`
- Modify: `src/services/db.py` (add `closing_line REAL` to the `new_columns` migration list)
- Test: `tests/test_closing_lines.py`

**Interfaces:**
- Consumes: `fetch_live_board()` from `extractors/pp_extractors.py`; `predictions` table.
- Produces: `update_closing_lines(board_df, db_path) -> int` (rows updated); `closing_line` column consumed by `scripts/season_report.py` (add a CLV section there in this task).

- [ ] **Step 1: Write the failing test**

`tests/test_closing_lines.py`:

```python
import sqlite3
import sys, os
import pandas as pd
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from capture_closing_lines import update_closing_lines


def test_updates_pending_rows_only(tmp_db):
    conn = sqlite3.connect(tmp_db)
    conn.execute("""INSERT INTO predictions
        (game_date, player, stat_type, line, play, status)
        VALUES ('2026-11-01', 'Test Guy', 'Points', 20.5, 'UNDER', 'PENDING')""")
    conn.execute("""INSERT INTO predictions
        (game_date, player, stat_type, line, play, status)
        VALUES ('2026-10-30', 'Old Guy', 'Points', 11.5, 'UNDER', 'WIN')""")
    conn.commit()

    board = pd.DataFrame([
        {"Player": "Test Guy", "Stat": "Points", "Line": 19.5, "Game Date": "2026-11-01"},
        {"Player": "Old Guy", "Stat": "Points", "Line": 12.5, "Game Date": "2026-10-30"},
    ])
    updated = update_closing_lines(board, tmp_db)
    assert updated == 1
    row = conn.execute("SELECT closing_line FROM predictions "
                       "WHERE player='Test Guy'").fetchone()
    assert row[0] == 19.5
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_closing_lines.py -v` → FAIL.

- [ ] **Step 3: Implement**

Add `("closing_line", "REAL"),` to the `new_columns` list in `init_db`.

`scripts/capture_closing_lines.py`:

```python
"""Run near first game lock (cron): stamp pending predictions with the
current PrizePicks line so we can measure closing-line value (CLV)."""
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
DB = os.path.join(ROOT, "sharp_edge.db")


def update_closing_lines(board_df, db_path=DB):
    conn = sqlite3.connect(db_path)
    updated = 0
    for _, row in board_df.iterrows():
        cur = conn.execute(
            """UPDATE predictions SET closing_line = ?
               WHERE player = ? AND stat_type = ? AND game_date = ?
                 AND status = 'PENDING'""",
            (float(row["Line"]), row["Player"], row["Stat"], row.get("Game Date")))
        updated += cur.rowcount
    conn.commit()
    conn.close()
    return updated


if __name__ == "__main__":
    from extractors.pp_extractors import fetch_live_board
    from services.db import init_db
    init_db()  # ensure closing_line column exists
    board = fetch_live_board()
    if board.empty:
        print("Board empty — nothing to capture.")
    else:
        print(f"Updated {update_closing_lines(board)} pending rows with closing lines.")
```

Also add a CLV section to `scripts/season_report.py`:

```python
    parts.append(_section(conn, "CLV (line moved toward us = positive signal)",
        "SELECT play, COUNT(*), "
        "ROUND(AVG(CASE WHEN play='UNDER' THEN closing_line - line "
        "ELSE line - closing_line END), 2) AS avg_clv "
        "FROM predictions WHERE closing_line IS NOT NULL GROUP BY play"))
```

- [ ] **Step 4: Run full suite**

Run: `python -m pytest -v` → all pass.

- [ ] **Step 5: Commit + cron note**

```bash
git add scripts/capture_closing_lines.py scripts/season_report.py src/services/db.py tests/test_closing_lines.py
git commit -m "feat: closing-line capture for CLV tracking"
```

Add to crontab (next to the existing pipeline entry): `50 18 * * * cd <repo> && python scripts/capture_closing_lines.py >> cron.log 2>&1` (≈10 min before typical first tip; adjust to slate).

---

### Task 12: Probability calibration (fix the 15%-cap pileup)

50% of bets hit the 15% edge cap because the NegBin/Poisson probabilities are overconfident. Fit an isotonic calibration curve on the 1,665 graded bets; apply it before edge computation.

**Files:**
- Create: `scripts/fit_calibration.py`
- Modify: `src/engine/probability.py` (add `calibrate_prob`), `src/main.py` (apply after `calculate_probabilities`)
- Test: `tests/test_calibration.py`

**Interfaces:**
- Produces: `calibrate_prob(p: float) -> float` (0–100 scale in and out; identity if `models/calibration.pkl` absent); `models/calibration.pkl` (joblib-dumped `IsotonicRegression`).

- [ ] **Step 1: Write the failing test**

`tests/test_calibration.py`:

```python
import numpy as np
import joblib
import engine.probability as prob


def test_identity_when_no_model(monkeypatch, tmp_path):
    monkeypatch.setattr(prob, "_CALIBRATION_PATH", str(tmp_path / "none.pkl"))
    monkeypatch.setattr(prob, "_CALIBRATOR", None)
    monkeypatch.setattr(prob, "_CALIBRATOR_LOADED", False)
    assert prob.calibrate_prob(63.0) == 63.0


def test_applies_fitted_model(monkeypatch, tmp_path):
    from sklearn.isotonic import IsotonicRegression
    # Synthetic: model that says "predicted p is 10 points too high"
    x = np.linspace(0.4, 0.9, 50)
    iso = IsotonicRegression(y_min=0, y_max=1, out_of_bounds="clip").fit(x, x - 0.10)
    path = tmp_path / "cal.pkl"
    joblib.dump(iso, path)
    monkeypatch.setattr(prob, "_CALIBRATION_PATH", str(path))
    monkeypatch.setattr(prob, "_CALIBRATOR", None)
    monkeypatch.setattr(prob, "_CALIBRATOR_LOADED", False)
    assert abs(prob.calibrate_prob(70.0) - 60.0) < 1.0
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_calibration.py -v` → FAIL (attrs missing).

- [ ] **Step 3: Implement**

In `src/engine/probability.py` add:

```python
import os
import joblib

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_CALIBRATION_PATH = os.path.join(_PROJECT_ROOT, "models", "calibration.pkl")
_CALIBRATOR = None
_CALIBRATOR_LOADED = False


def calibrate_prob(p):
    """Map raw model probability (0-100) to calibrated probability using the
    isotonic curve fit on graded history. Identity if no curve is fitted."""
    global _CALIBRATOR, _CALIBRATOR_LOADED
    if not _CALIBRATOR_LOADED:
        _CALIBRATOR_LOADED = True
        if os.path.exists(_CALIBRATION_PATH):
            _CALIBRATOR = joblib.load(_CALIBRATION_PATH)
            logger.info("[+] Probability calibrator loaded.")
    if _CALIBRATOR is None:
        return p
    return float(_CALIBRATOR.predict([p / 100.0])[0] * 100.0)
```

`scripts/fit_calibration.py`:

```python
"""Fit isotonic calibration: raw model probability -> realized win frequency.
Prints a reliability table, saves models/calibration.pkl."""
import os
import sqlite3
import joblib
import numpy as np
from sklearn.isotonic import IsotonicRegression

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
rows = sqlite3.connect(os.path.join(ROOT, "sharp_edge.db")).execute(
    "SELECT poisson_prob, status='WIN' FROM predictions "
    "WHERE status IN ('WIN','LOSS') AND poisson_prob IS NOT NULL").fetchall()
p = np.array([r[0] for r in rows]) / 100.0
y = np.array([r[1] for r in rows], dtype=float)
print(f"Fitting on {len(p)} graded bets")

for lo in np.arange(0.50, 0.90, 0.05):
    mask = (p >= lo) & (p < lo + 0.05)
    if mask.sum() >= 20:
        print(f"  predicted {lo:.2f}-{lo+0.05:.2f}: realized "
              f"{y[mask].mean():.3f} (n={mask.sum()})")

iso = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip").fit(p, y)
out = os.path.join(ROOT, "models", "calibration.pkl")
joblib.dump(iso, out)
print(f"Saved {out}")
```

In `src/main.py`, right after `implied_prob` is chosen:

```python
        implied_prob = calibrate_prob(implied_prob)
```

(import `calibrate_prob` alongside the other `engine.probability` imports). Keep `MAX_EDGE_CAP = 15.0` for now — after 2+ weeks of calibrated live data, check what fraction still hits the cap; it should be <10%, at which point raise it to 20.

- [ ] **Step 4: Run tests, fit the real curve**

Run: `python -m pytest tests/test_calibration.py -v` → 2 passed.
Run: `python scripts/fit_calibration.py` → reliability table prints (expect predicted 0.65–0.70 → realized ≈ 0.60–0.62), `models/calibration.pkl` saved.

- [ ] **Step 5: Commit**

```bash
git add scripts/fit_calibration.py src/engine/probability.py src/main.py tests/test_calibration.py
git commit -m "feat: isotonic probability calibration from graded history"
```

---

### Task 13: Backtest harness

Replay any strategy config against the graded DB so filter changes are validated on history, not live money. Limitation (document it in the script docstring): it can only *tighten* filters over already-logged plays — it can't discover plays the old filters excluded.

**Files:**
- Create: `scripts/backtest.py`
- Test: `tests/test_backtest.py`

**Interfaces:**
- Consumes: `evaluate_play` (Task 7 signature), `compute_flex_roi` + `PAYOUTS` (Task 5), `predictions` table.
- Produces: `backtest(rows, evaluator) -> dict` with keys `n`, `wins`, `win_rate`, `roi_flex5` — rows are `(stat_type, play, ev_edge, player, vegas_confirms, status)` tuples.

- [ ] **Step 1: Write the failing test**

`tests/test_backtest.py`:

```python
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from backtest import backtest


def test_backtest_filters_and_scores():
    rows = [
        ("Rebounds", "UNDER", 10.0, "A", None, "WIN"),
        ("Rebounds", "UNDER", 4.0, "B", None, "WIN"),   # below 8% floor
        ("Points", "UNDER", 12.0, "C", None, "LOSS"),
        ("Points", "UNDER", 9.0, "D", False, "LOSS"),   # vegas diverges
    ]
    def evaluator(stat, play, edge, player, vegas):
        from engine.strategy import evaluate_play
        return evaluate_play(stat, play, edge, player_name=player,
                             vegas_confirms=vegas)[0]
    result = backtest(rows, evaluator)
    assert result["n"] == 2          # B and D filtered out
    assert result["wins"] == 1
    assert result["win_rate"] == 50.0
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_backtest.py -v` → FAIL.

- [ ] **Step 3: Implement**

`scripts/backtest.py`:

```python
"""Replay strategy configs against graded history.
LIMITATION: only tightens filters over logged plays; it cannot surface
plays the production filters excluded (those were never logged).
Usage: python scripts/backtest.py"""
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from season_report import compute_flex_roi  # noqa: E402


def load_rows(db_path=os.path.join(ROOT, "sharp_edge.db")):
    return sqlite3.connect(db_path).execute(
        "SELECT stat_type, play, ev_edge, player, vegas_confirms, status "
        "FROM predictions WHERE status IN ('WIN','LOSS')").fetchall()


def backtest(rows, evaluator):
    kept = [r for r in rows
            if evaluator(r[0], r[1], r[2] or 0.0, r[3],
                         None if r[4] is None else bool(r[4]))]
    n = len(kept)
    wins = sum(1 for r in kept if r[5] == "WIN")
    wr = round(100.0 * wins / n, 1) if n else 0.0
    return {"n": n, "wins": wins, "win_rate": wr,
            "roi_flex5": compute_flex_roi(wr / 100.0, "flex5") if n else 0.0}


if __name__ == "__main__":
    from engine.strategy import evaluate_play
    rows = load_rows()

    def current(stat, play, edge, player, vegas):
        return evaluate_play(stat, play, edge, player_name=player,
                             vegas_confirms=vegas)[0]

    baseline = {"n": len(rows),
                "wins": sum(1 for r in rows if r[5] == "WIN")}
    baseline["win_rate"] = round(100.0 * baseline["wins"] / baseline["n"], 1)
    print(f"All logged bets:      {baseline}")
    print(f"Current strategy:     {backtest(rows, current)}")
```

- [ ] **Step 4: Run tests + run against real history**

Run: `python -m pytest tests/test_backtest.py -v` → passed.
Run: `python scripts/backtest.py` → expect current strategy (8% floor + Vegas gate) to show a higher WR than the 60.2% baseline on fewer bets. Record the numbers in the commit message.

- [ ] **Step 5: Commit**

```bash
git add scripts/backtest.py tests/test_backtest.py
git commit -m "feat: backtest harness — validate filter changes on graded history"
```

---

### Task 14: Venue abstraction

Prepare for Kalshi (and the NFL plan) by putting board-fetching behind an interface. PrizePicks becomes the first adapter; `main.py` consumes the interface.

**Files:**
- Create: `src/venues/__init__.py`, `src/venues/base.py`, `src/venues/prizepicks.py`
- Modify: `src/main.py` (use `get_venue("prizepicks").fetch_board("NBA")`)
- Test: `tests/test_venues.py`

**Interfaces:**
- Produces: `VenueAdapter` ABC with `name: str`, `fetch_board(league: str) -> pd.DataFrame` (columns: `Player, Stat, Line, Matchup, Game Date`); `get_venue(name: str) -> VenueAdapter`. The NFL plan's Task 4 and Kalshi Task 15 implement against this.

- [ ] **Step 1: Write the failing test**

`tests/test_venues.py`:

```python
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
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_venues.py -v` → FAIL (module missing).

- [ ] **Step 3: Implement**

`src/venues/base.py`:

```python
from abc import ABC, abstractmethod
import pandas as pd

BOARD_COLUMNS = ["Player", "Stat", "Line", "Matchup", "Game Date"]


class VenueAdapter(ABC):
    name: str

    @abstractmethod
    def fetch_board(self, league: str) -> pd.DataFrame:
        """Return the live prop board with at least BOARD_COLUMNS."""
```

`src/venues/prizepicks.py`:

```python
from extractors.pp_extractors import fetch_live_board
from venues.base import VenueAdapter


class PrizePicksAdapter(VenueAdapter):
    name = "prizepicks"

    def fetch_board(self, league="NBA"):
        # league param plumbed through in the NFL plan (extractor currently
        # hardcodes the NBA filter).
        return fetch_live_board()
```

`src/venues/__init__.py`:

```python
from venues.prizepicks import PrizePicksAdapter

_REGISTRY = {a.name: a for a in [PrizePicksAdapter()]}


def get_venue(name):
    return _REGISTRY[name]
```

In `src/main.py`, replace `pp_board = fetch_live_board()` with:

```python
    from venues import get_venue
    pp_board = get_venue(os.getenv("VENUE", "prizepicks")).fetch_board("NBA")
```

- [ ] **Step 4: Run full suite**

Run: `python -m pytest -v` → all pass.

- [ ] **Step 5: Commit**

```bash
git add src/venues/ src/main.py tests/test_venues.py
git commit -m "feat: venue abstraction — PrizePicks behind VenueAdapter"
```

---

### Task 15: Kalshi read-only market client

Read-only market data: discover NBA-related markets, normalize to `(title, ticker, market_prob, fee)`. No trading. **At execution: pull current Kalshi API docs (Context7 or https://docs.kalshi.com) first — verify base URL, market-listing params, and the fee schedule; the code below reflects the API as of early 2026.**

**Files:**
- Create: `src/venues/kalshi.py`
- Create: `tests/fixtures/kalshi_markets.json`
- Test: `tests/test_kalshi.py`

**Interfaces:**
- Produces: `get_markets(series_ticker=None, status="open") -> list[dict]` (raw API market dicts); `normalize_market(m: dict) -> dict` with keys `ticker, title, market_prob, yes_bid, yes_ask`; `taker_fee_cents(price_cents: int, contracts: int = 1) -> int`. Consumed by Task 16.

- [ ] **Step 1: Create fixture and failing tests**

`tests/fixtures/kalshi_markets.json`:

```json
{
  "markets": [
    {
      "ticker": "KXNBAPTS-26NOV01LALJAMES-24",
      "title": "Will LeBron James score 25+ points?",
      "status": "open",
      "yes_bid": 55,
      "yes_ask": 59
    }
  ]
}
```

`tests/test_kalshi.py`:

```python
import json
import os
from venues.kalshi import normalize_market, taker_fee_cents

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "kalshi_markets.json")


def test_normalize_uses_bid_ask_mid():
    m = json.load(open(FIXTURE))["markets"][0]
    norm = normalize_market(m)
    assert norm["ticker"].startswith("KXNBAPTS")
    assert norm["market_prob"] == 57.0  # (55+59)/2


def test_taker_fee_at_50_cents_is_2_cents():
    # ceil(0.07 * 1 * 0.50 * 0.50 * 100) = 2 cents. Verify factor vs current
    # Kalshi fee schedule at execution time.
    assert taker_fee_cents(50) == 2


def test_no_quotes_returns_none_prob():
    assert normalize_market({"ticker": "X", "title": "t"})["market_prob"] is None
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_kalshi.py -v` → FAIL.

- [ ] **Step 3: Implement**

`src/venues/kalshi.py`:

```python
"""Kalshi read-only market data client (no trading).
Docs: https://docs.kalshi.com — verify base URL/params/fees before relying
on this in production; API surface was last verified 2026-07."""
import math
import os
import requests
from utils.utils import logger

KALSHI_BASE = os.getenv("KALSHI_API_BASE",
                        "https://api.elections.kalshi.com/trade-api/v2")


def get_markets(series_ticker=None, status="open", limit=200):
    """Public market-data endpoint (no auth required for reads)."""
    params = {"status": status, "limit": limit}
    if series_ticker:
        params["series_ticker"] = series_ticker
    try:
        resp = requests.get(f"{KALSHI_BASE}/markets", params=params, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"[!] Kalshi markets HTTP {resp.status_code}")
            return []
        return resp.json().get("markets", [])
    except Exception as e:
        logger.warning(f"[!] Kalshi request failed: {e}")
        return []


def normalize_market(m):
    yes_bid, yes_ask = m.get("yes_bid"), m.get("yes_ask")
    prob = None
    if yes_bid is not None and yes_ask is not None and (yes_bid or yes_ask):
        prob = round((yes_bid + yes_ask) / 2.0, 1)  # cents == implied %
    return {"ticker": m.get("ticker", ""), "title": m.get("title", ""),
            "market_prob": prob, "yes_bid": yes_bid, "yes_ask": yes_ask}


def taker_fee_cents(price_cents, contracts=1):
    """Kalshi general fee: ceil(0.07 * C * P * (1-P)) per side, P in dollars."""
    p = price_cents / 100.0
    return math.ceil(0.07 * contracts * p * (1 - p) * 100)
```

- [ ] **Step 4: Run tests + live smoke test**

Run: `python -m pytest tests/test_kalshi.py -v` → 3 passed.
Run: `python -c "import sys; sys.path.insert(0,'src'); from venues.kalshi import get_markets; ms=get_markets(limit=5); print(len(ms), ms[0]['ticker'] if ms else '')"` → prints a count with no traceback. Then explore: list series matching NBA/NFL to find the player-prop series tickers and record them in the module docstring.

- [ ] **Step 5: Commit**

```bash
git add src/venues/kalshi.py tests/
git commit -m "feat: Kalshi read-only market data client with fee model"
```

---

### Task 16: Kalshi paper-trade logger + report

Compare our calibrated probabilities against Kalshi market prices where markets overlap our props; log hypothetical edges net of fees. No real orders. Decision gate: only consider funding an account after ≥1 month of paper signals shows positive net edge.

**Files:**
- Create: `scripts/kalshi_paper.py`
- Modify: `src/services/db.py` (create `kalshi_signals` table in `init_db`)
- Test: `tests/test_kalshi_paper.py`

**Interfaces:**
- Consumes: `normalize_market`, `taker_fee_cents` (Task 15); `calibrate_prob` (Task 12).
- Produces: `log_signal(db_path, ticker, title, our_prob, market_prob) -> None`; table `kalshi_signals(id, ts, market_ticker, title, our_prob, market_prob, edge_net_fees, result)`.

- [ ] **Step 1: Add the table to `init_db` and write the failing test**

In `init_db`, after the predictions DDL:

```python
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS kalshi_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT,
            market_ticker TEXT,
            title TEXT,
            our_prob REAL,
            market_prob REAL,
            edge_net_fees REAL,
            result TEXT DEFAULT 'PENDING'
        )
    ''')
```

`tests/test_kalshi_paper.py`:

```python
import sqlite3
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from kalshi_paper import net_edge, log_signal


def test_net_edge_subtracts_fee():
    # our prob 65%, market 57% -> gross 8 pts, minus taker fee at 57c
    e = net_edge(our_prob=65.0, market_prob=57.0)
    assert 5.0 < e < 8.0


def test_log_signal_inserts(tmp_db):
    log_signal(tmp_db, "KXTEST-1", "test market", 65.0, 57.0)
    n = sqlite3.connect(tmp_db).execute(
        "SELECT COUNT(*) FROM kalshi_signals").fetchone()[0]
    assert n == 1
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_kalshi_paper.py -v` → FAIL.

- [ ] **Step 3: Implement**

`scripts/kalshi_paper.py`:

```python
"""Paper-trade Kalshi: log (our calibrated prob) vs (market price) for
overlapping player-prop markets. No orders are ever placed.
Usage: python scripts/kalshi_paper.py <series_ticker>"""
import os
import sqlite3
import sys
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from venues.kalshi import get_markets, normalize_market, taker_fee_cents  # noqa: E402

DB = os.path.join(ROOT, "sharp_edge.db")


def net_edge(our_prob, market_prob):
    """Percentage-point edge buying YES at market_prob, net of taker fee."""
    fee_pts = taker_fee_cents(int(round(market_prob)))  # cents/contract == pts
    return round(our_prob - market_prob - fee_pts, 2)


def log_signal(db_path, ticker, title, our_prob, market_prob):
    conn = sqlite3.connect(db_path)
    conn.execute(
        """INSERT INTO kalshi_signals
           (ts, market_ticker, title, our_prob, market_prob, edge_net_fees)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (datetime.now().isoformat(timespec="seconds"), ticker, title,
         our_prob, market_prob, net_edge(our_prob, market_prob)))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    series = sys.argv[1] if len(sys.argv) > 1 else None
    for m in get_markets(series_ticker=series):
        norm = normalize_market(m)
        if norm["market_prob"] is None:
            continue
        # Wiring our projection to a specific market requires parsing the
        # market title into (player, stat, line) — do this per-series once
        # the NBA series tickers are recorded in venues/kalshi.py (Task 15
        # step 4). Until then this logs market data only:
        print(norm["ticker"], norm["title"], norm["market_prob"])
```

- [ ] **Step 4: Run full suite**

Run: `python -m pytest -v` → all pass.

- [ ] **Step 5: Commit**

```bash
git add scripts/kalshi_paper.py src/services/db.py tests/test_kalshi_paper.py
git commit -m "feat: Kalshi paper-trade signal logging (read-only)"
```

---

## Self-Review Notes

- Spec coverage: all Phase 0–3 items from the roadmap have tasks except "model minutes explicitly" (deferred — research-heavy, needs calibrated live data first to justify; revisit after 4 weeks of calibrated results).
- Type consistency: `evaluate_play(..., vegas_confirms=None)` defined in Task 7, consumed in Task 13; `compute_flex_roi` defined in Task 5, consumed in Task 13; `tmp_db` from Task 1 used throughout; `calibrate_prob` defined Task 12, consumed Task 16 (via kalshi_paper's eventual projection wiring).
- Ordering: Tasks 1–5 must run in order. Tasks 6–13 are order-independent after Task 5. Tasks 14–16 are sequential.
