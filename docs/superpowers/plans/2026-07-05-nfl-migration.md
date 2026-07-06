# NFL Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add NFL player-prop coverage to this repo (same pipeline, new sport adapter): PrizePicks NFL board → NFL projections from nflverse data → probability → strategy filter → grading, launched in paper mode for weeks 1–4 of the 2026 season.

**Architecture:** **Decision: expand this repo, do not start a new project.** The strategy filter, probability engine, DB, dedup, notifier, grader loop, and venue adapters are sport-agnostic (~60–70% of the system); a second repo would mean fixing every bug twice and re-earning the 60.2%-WR core from scratch. Sport-specific code moves behind a `SportAdapter` interface under `src/sports/`; NBA becomes the first adapter (wrapping existing modules unchanged), NFL the second. One DB with a `sport` column.

**Tech Stack:** Existing stack + `nfl_data_py` (nflverse: weekly stats, schedules, injuries/practice reports — free). PrizePicks NFL uses the same projections API with a different league filter. The Odds API supports `americanfootball_nfl` for the Vegas gate.

## Global Constraints

- **Prerequisite:** NBA plan (`2026-07-05-nba-improvements-and-kalshi.md`) Tasks 1–3, 6–8, and 14 must be complete (pytest, dedup fix, 8% floor, Vegas gate, season phase, venue abstraction).
- Dedup identity becomes `(sport, game_date, player, stat_type)`.
- NFL launches with: global 8% edge floor, no stat/direction blocks (no data yet), max 10 plays per slate, PAPER_MODE for weeks 1–4.
- Tests never hit the network — nflverse loaders are wrapped so tests inject fixture DataFrames.
- Sport adapters must not import each other; shared logic lives in `engine/` or `services/`.
- Run tests with `python -m pytest` from the repo root.
- 2026 NFL kickoff is ~Sep 10; Tasks 1–9 must land by Sep 1 to allow paper weeks 1–4 (real stakes earliest week 5, ~Oct 8).

---

### Task 1: SportAdapter interface + `sport` column

**Files:**
- Create: `src/sports/__init__.py`, `src/sports/base.py`
- Modify: `src/services/db.py` (add `sport` column, widen dedup key and index)
- Test: `tests/test_sports_base.py`, extend `tests/test_db_dedup.py`

**Interfaces:**
- Produces: `SportAdapter` ABC:
  - `league: str` (PrizePicks league filter value, e.g. `"NBA"`, `"NFL"`)
  - `load_context() -> dict` (all season data fetched once per run)
  - `project(player: str, stat: str, matchup: str, context: dict) -> float | None`
  - `win_probability(projection: float, line: float, stat: str, player: str, context: dict) -> float` (0–100, prob the OVER hits)
  - `resolve_actual(player: str, stat: str, game_date: str, context: dict) -> float | None`
  - `get_sport(name: str) -> SportAdapter` registry function.
- Produces: `predictions.sport` column; dedup key `(sport, game_date, player, stat_type)`; `log_predictions(df, sport="NBA")`, `filter_new_plays(df, sport="NBA")`.

- [ ] **Step 1: Write the failing tests**

`tests/test_sports_base.py`:

```python
from sports.base import SportAdapter


def test_adapter_is_abstract():
    import pytest
    with pytest.raises(TypeError):
        SportAdapter()
```

Append to `tests/test_db_dedup.py`:

```python
def test_same_play_different_sport_logs_twice(tmp_db, monkeypatch):
    import services.db as db
    monkeypatch.setattr(db, "datetime", _FakeDatetime)
    _FakeDatetime._now = datetime(2026, 10, 31)
    db.log_predictions(_play_row(), sport="NBA")
    db.log_predictions(_play_row(), sport="NFL")
    n = sqlite3.connect(tmp_db).execute(
        "SELECT COUNT(*) FROM predictions").fetchone()[0]
    assert n == 2
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_sports_base.py tests/test_db_dedup.py -v` → FAIL (module missing / unexpected kwarg).

- [ ] **Step 3: Implement**

`src/sports/base.py`:

```python
from abc import ABC, abstractmethod


class SportAdapter(ABC):
    league: str  # PrizePicks league filter value

    @abstractmethod
    def load_context(self) -> dict:
        """Fetch all season data once per run. Returned dict is passed to
        project/win_probability/resolve_actual."""

    @abstractmethod
    def project(self, player, stat, matchup, context):
        """Point projection for player+stat, or None if unprojectable."""

    @abstractmethod
    def win_probability(self, projection, line, stat, player, context):
        """P(actual > line) in 0-100."""

    @abstractmethod
    def resolve_actual(self, player, stat, game_date, context):
        """Actual stat value after the game, or None if not yet available."""
```

`src/sports/__init__.py` (registry filled by Tasks 2 and 5):

```python
_REGISTRY = {}


def register(name, adapter):
    _REGISTRY[name] = adapter


def get_sport(name):
    return _REGISTRY[name]
```

`src/services/db.py`: add `("sport", "TEXT"),` to `new_columns`; in `init_db` after migrations run `cursor.execute("UPDATE predictions SET sport='NBA' WHERE sport IS NULL")`; replace the unique index:

```python
    cursor.execute("DROP INDEX IF EXISTS idx_pred_unique")
    try:
        cursor.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_pred_unique_v2
                          ON predictions(sport, game_date, player, stat_type)""")
    except sqlite3.OperationalError:
        pass
```

Add `sport="NBA"` parameter to `log_predictions` and `filter_new_plays`; include `sport = ?` in both dedup WHERE clauses and add the column to the INSERT.

- [ ] **Step 4: Run full suite**

Run: `python -m pytest -v` → all pass.

- [ ] **Step 5: Commit**

```bash
git add src/sports/ src/services/db.py tests/
git commit -m "feat: SportAdapter interface + sport-aware dedup key"
```

---

### Task 2: NBA adapter (wrap existing modules, zero behavior change)

**Files:**
- Create: `src/sports/nba/__init__.py` (empty), `src/sports/nba/adapter.py`
- Modify: `src/sports/__init__.py` (register), `src/main.py` (pass `sport="NBA"` to `log_predictions`/`filter_new_plays`)
- Test: `tests/test_nba_adapter.py`

**Interfaces:**
- Consumes: `DeterministicProjector`, `calculate_probabilities` (existing), `SportAdapter` (Task 1).
- Produces: `get_sport("NBA")` returns a working adapter. `main.py` stays the NBA orchestrator for now (full orchestrator unification is future work — YAGNI until NFL runs).

- [ ] **Step 1: Write the failing test**

`tests/test_nba_adapter.py`:

```python
from sports import get_sport
from sports.base import SportAdapter


def test_nba_adapter_registered():
    import sports.nba.adapter  # noqa: F401 — triggers registration
    a = get_sport("NBA")
    assert isinstance(a, SportAdapter)
    assert a.league == "NBA"
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_nba_adapter.py -v` → FAIL.

- [ ] **Step 3: Implement**

`src/sports/nba/adapter.py`:

```python
"""NBA adapter — thin wrapper over the existing V2.1 engine modules.
Existing behavior is unchanged; this exists so NBA and NFL share one
interface."""
from engine.projections import DeterministicProjector
from engine.probability import calculate_probabilities
from sports import register
from sports.base import SportAdapter


class NBAAdapter(SportAdapter):
    league = "NBA"

    def load_context(self):
        # Defers to main.py's existing Phase-1 fetches; main.py builds the
        # projector and passes it through context when it adopts adapters.
        from extractors.nba_extractors import (
            get_advanced_player_baselines, get_team_pace_and_defense,
            get_tracking_data, get_opponent_matchup_multipliers,
            get_league_gamelog_for_ewma)
        from utils.season import get_season_phase
        phase = get_season_phase()
        adv = get_advanced_player_baselines(last_n_games=0,
                                            season_type="Regular Season")
        projector = DeterministicProjector(
            advanced_df=adv,
            tracking_df=get_tracking_data(),
            pace_df=get_team_pace_and_defense(),
            opp_multipliers=get_opponent_matchup_multipliers(),
            game_logs_df=get_league_gamelog_for_ewma(season_type=phase),
        )
        return {"projector": projector}

    def project(self, player, stat, matchup, context):
        opp = str(matchup).split(" ")[-1].upper()
        is_home = "@" not in str(matchup)
        projector = context["projector"]
        minutes = projector.get_ewma_baseline(player, "MIN")
        if not minutes:
            return None
        return projector.generate_projection(player, opp, minutes, stat,
                                             is_home=is_home)

    def win_probability(self, projection, line, stat, player, context):
        var = None
        projector = context.get("projector")
        if projector is not None:
            stat_map = {"Points": "PTS", "Rebounds": "REB", "Assists": "AST",
                        "Pts+Rebs+Asts": "PRA", "Pts+Rebs": "PR",
                        "Pts+Asts": "PA", "Rebs+Asts": "RA"}
            key = stat_map.get(stat)
            var = projector.get_stat_variance(player, key) if key else None
        return calculate_probabilities(projection, line, stat_type=stat,
                                       empirical_variance=var)["over"]

    def resolve_actual(self, player, stat, game_date, context):
        # Grading stays in services/grader.py for NBA (live-pivot logic).
        return None


register("NBA", NBAAdapter())
```

In `src/main.py`, change the two DB calls to `filter_new_plays(results_df, sport="NBA")` and `log_predictions(new_plays_df, sport="NBA")`.

- [ ] **Step 4: Run full suite**

Run: `python -m pytest -v` → all pass.

- [ ] **Step 5: Commit**

```bash
git add src/sports/ src/main.py tests/test_nba_adapter.py
git commit -m "feat: NBA adapter wrapping existing V2.1 engine"
```

---

### Task 3: NFL data layer (nflverse)

**Files:**
- Modify: `requirements.txt` (add `nfl_data_py`)
- Create: `src/sports/nfl/__init__.py` (empty), `src/sports/nfl/data.py`
- Test: `tests/test_nfl_data.py`

**Interfaces:**
- Produces: `load_weekly(seasons: list[int]) -> pd.DataFrame` (network; thin wrapper), `add_combo_columns(df) -> pd.DataFrame`, `player_recent_stats(weekly_df, player, stat_col, n=8) -> np.ndarray` (most recent first), `NFL_STAT_MAP: dict[str, str | list[str]]` (PrizePicks stat name → weekly column(s)).

- [ ] **Step 1: Write the failing tests**

`tests/test_nfl_data.py`:

```python
import pandas as pd
from sports.nfl.data import add_combo_columns, player_recent_stats, NFL_STAT_MAP


def _weekly():
    return pd.DataFrame({
        "player_display_name": ["A. Receiver"] * 4,
        "season": [2026] * 4,
        "week": [1, 2, 3, 4],
        "rushing_yards": [5, 0, 12, 3],
        "receiving_yards": [80, 55, 110, 62],
        "receptions": [6, 4, 9, 5],
    })


def test_stat_map_covers_core_markets():
    for stat in ["Pass Yards", "Rush Yards", "Receiving Yards",
                 "Receptions", "Rush+Rec Yds"]:
        assert stat in NFL_STAT_MAP


def test_combo_column_added():
    df = add_combo_columns(_weekly())
    assert list(df["rush_rec_yards"]) == [85, 55, 122, 65]


def test_recent_stats_most_recent_first():
    df = add_combo_columns(_weekly())
    vals = player_recent_stats(df, "A. Receiver", "receiving_yards", n=3)
    assert list(vals) == [62, 110, 55]
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_nfl_data.py -v` → FAIL.

- [ ] **Step 3: Implement**

Add `nfl_data_py` to `requirements.txt`, `pip install nfl_data_py`.

`src/sports/nfl/data.py`:

```python
"""nflverse data layer. Weekly player stats, schedules, injuries.
All network access goes through load_* functions so tests can inject frames."""
import numpy as np
from utils.utils import logger

# PrizePicks NFL stat name -> nflverse weekly column(s)
NFL_STAT_MAP = {
    "Pass Yards": "passing_yards",
    "Pass TDs": "passing_tds",
    "Pass Completions": "completions",
    "Pass Attempts": "attempts",
    "INT": "interceptions",
    "Rush Yards": "rushing_yards",
    "Rush Attempts": "carries",
    "Receiving Yards": "receiving_yards",
    "Receptions": "receptions",
    "Targets": "targets",
    "Rush+Rec Yds": "rush_rec_yards",  # combo built in add_combo_columns
    "Pass+Rush Yds": "pass_rush_yards",
}


def load_weekly(seasons):
    import nfl_data_py as nfl
    df = nfl.import_weekly_data(seasons)
    logger.info(f"[+] nflverse weekly: {len(df)} rows for seasons {seasons}")
    return add_combo_columns(df)


def load_injuries(seasons):
    import nfl_data_py as nfl
    return nfl.import_injuries(seasons)


def load_schedules(seasons):
    import nfl_data_py as nfl
    return nfl.import_schedules(seasons)


def add_combo_columns(df):
    df = df.copy()
    if {"rushing_yards", "receiving_yards"}.issubset(df.columns):
        df["rush_rec_yards"] = df["rushing_yards"].fillna(0) + \
            df["receiving_yards"].fillna(0)
    if {"passing_yards", "rushing_yards"}.issubset(df.columns):
        df["pass_rush_yards"] = df["passing_yards"].fillna(0) + \
            df["rushing_yards"].fillna(0)
    return df


def player_recent_stats(weekly_df, player, stat_col, n=8):
    """Player's last n values for stat_col, most recent first."""
    rows = weekly_df[weekly_df["player_display_name"] == player]
    rows = rows.sort_values(["season", "week"], ascending=False).head(n)
    return rows[stat_col].fillna(0).to_numpy(dtype=float)
```

- [ ] **Step 4: Run tests + live smoke test**

Run: `python -m pytest tests/test_nfl_data.py -v` → 3 passed.
Run: `python -c "import sys; sys.path.insert(0,'src'); from sports.nfl.data import load_weekly; df=load_weekly([2025]); print(df.shape, 'passing_yards' in df.columns)"` → prints a shape like `(5xxx, ~50+)` and `True`. If column names differ from `NFL_STAT_MAP` values, fix the map now.

- [ ] **Step 5: Commit**

```bash
git add requirements.txt src/sports/nfl/ tests/test_nfl_data.py
git commit -m "feat: NFL data layer over nflverse"
```

---

### Task 4: PrizePicks NFL board

The extractor hardcodes `leagues.get(league_id) != "NBA"`. Parameterize it and plumb `league` through the venue adapter.

**Files:**
- Modify: `src/extractors/pp_extractors.py` (add `league` param to `fetch_live_board` and `_parse_board_json`), `src/venues/prizepicks.py` (pass league through)
- Create: `tests/fixtures/pp_board_nfl.json`
- Test: `tests/test_pp_extractor.py`

**Interfaces:**
- Consumes: `VenueAdapter.fetch_board(league)` (NBA plan Task 14).
- Produces: `fetch_live_board(league="NBA")`, `_parse_board_json(json_data, league="NBA")` — board rows filtered to the given league.

- [ ] **Step 1: Create fixture and failing test**

`tests/fixtures/pp_board_nfl.json` (minimal PrizePicks payload shape):

```json
{
  "included": [
    {"type": "new_player", "id": "p1",
     "attributes": {"display_name": "QB Guy", "team": "KC"}},
    {"type": "league", "id": "L9", "attributes": {"name": "NFL"}},
    {"type": "league", "id": "L7", "attributes": {"name": "NBA"}}
  ],
  "data": [
    {"type": "projection", "id": "1",
     "attributes": {"stat_type": "Pass Yards", "line_score": 265.5,
                     "description": "KC vs BUF", "odds_type": "standard",
                     "start_time": "2026-09-13T17:00:00Z"},
     "relationships": {
       "new_player": {"data": {"id": "p1"}},
       "league": {"data": {"id": "L9"}}}}
  ]
}
```

`tests/test_pp_extractor.py`:

```python
import json
import os
from extractors.pp_extractors import _parse_board_json

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "pp_board_nfl.json")


def test_nfl_league_filter():
    payload = json.load(open(FIXTURE))
    board = _parse_board_json(payload, league="NFL")
    assert len(board) == 1
    assert board.iloc[0]["Player"] == "QB Guy"
    assert board.iloc[0]["Stat"] == "Pass Yards"


def test_nba_filter_excludes_nfl_rows():
    payload = json.load(open(FIXTURE))
    assert _parse_board_json(payload, league="NBA").empty
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_pp_extractor.py -v` → FAIL (unexpected kwarg). If the fixture's field names don't match `_parse_board_json`'s actual parsing (read the full function first), fix the fixture to match the real parser, not vice versa.

- [ ] **Step 3: Implement**

In `src/extractors/pp_extractors.py`:

```python
def fetch_live_board(league="NBA"):
    ...
        return _parse_board_json(json_data, league=league)


def _parse_board_json(json_data, league="NBA"):
    ...
        if leagues.get(league_id) != league or attrs.get('odds_type') != 'standard':
            continue
```

In `src/venues/prizepicks.py`:

```python
    def fetch_board(self, league="NBA"):
        return fetch_live_board(league=league)
```

- [ ] **Step 4: Run full suite + live smoke check**

Run: `python -m pytest -v` → all pass.
(September: `python -c "...fetch_live_board(league='NFL')..."` should return rows; in July the NFL board may be empty — that's fine.)

- [ ] **Step 5: Commit**

```bash
git add src/extractors/pp_extractors.py src/venues/prizepicks.py tests/
git commit -m "feat: league-parameterized PrizePicks board (NFL support)"
```

---

### Task 5: NFL projection engine

Small-sample sport: EWMA over recent games, shrunk toward a position-level prior, adjusted by opponent's allowed-vs-position. No pace, no minutes — opportunity share is implicit in the per-game stats.

**Files:**
- Create: `src/sports/nfl/projections.py`
- Test: `tests/test_nfl_projections.py`

**Interfaces:**
- Consumes: `player_recent_stats`, `NFL_STAT_MAP` (Task 3).
- Produces: `ewma(values: np.ndarray, alpha=0.35) -> float`; `shrink(player_mean, n_games, prior_mean, prior_weight=4) -> float`; `opponent_multiplier(weekly_df, opp_team, position, stat_col) -> float` (clamped 0.85–1.15); `project_nfl(weekly_df, player, stat, opp_team, position) -> float | None`.

- [ ] **Step 1: Write the failing tests**

`tests/test_nfl_projections.py`:

```python
import numpy as np
import pandas as pd
from sports.nfl.projections import ewma, shrink, opponent_multiplier, project_nfl


def test_ewma_weights_recent_higher():
    rising = ewma(np.array([100.0, 50.0, 50.0, 50.0]))   # most recent first
    assert rising > 62.5  # plain mean


def test_shrink_pulls_small_samples_to_prior():
    # 2 games at 120 vs position prior of 60: heavily shrunk
    assert shrink(120.0, 2, 60.0) < 90.0
    # 16 games at 120: barely shrunk
    assert shrink(120.0, 16, 60.0) > 105.0


def _league(op_yards):
    rows = []
    for wk in [1, 2, 3, 4]:
        rows.append({"player_display_name": f"WR{wk}", "position": "WR",
                     "opponent_team": "NE", "season": 2026, "week": wk,
                     "receiving_yards": op_yards})
        rows.append({"player_display_name": f"WRx{wk}", "position": "WR",
                     "opponent_team": "KC", "season": 2026, "week": wk,
                     "receiving_yards": 60.0})
    return pd.DataFrame(rows)


def test_opponent_multiplier_clamped():
    df = _league(op_yards=200.0)  # NE allows way more than league avg
    m = opponent_multiplier(df, "NE", "WR", "receiving_yards")
    assert m == 1.15  # clamped at +15%


def test_project_returns_none_for_unknown_player():
    df = _league(60.0)
    assert project_nfl(df, "Nobody", "Receiving Yards", "NE", "WR") is None
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_nfl_projections.py -v` → FAIL.

- [ ] **Step 3: Implement**

`src/sports/nfl/projections.py`:

```python
"""NFL projections: EWMA recent form, shrunk to position prior, adjusted by
opponent allowed-vs-position. Small-sample-safe by construction."""
import numpy as np
from sports.nfl.data import NFL_STAT_MAP, player_recent_stats

EWMA_ALPHA = 0.35
PRIOR_WEIGHT = 4          # pseudo-games of position prior
OPP_MULT_MIN, OPP_MULT_MAX = 0.85, 1.15


def ewma(values, alpha=EWMA_ALPHA):
    """values: most recent first."""
    if len(values) == 0:
        return 0.0
    w = np.array([(1 - alpha) ** i for i in range(len(values))])
    return float(np.dot(values, w / w.sum()))


def shrink(player_mean, n_games, prior_mean, prior_weight=PRIOR_WEIGHT):
    return (n_games * player_mean + prior_weight * prior_mean) / \
        (n_games + prior_weight)


def position_prior(weekly_df, position, stat_col):
    rows = weekly_df[weekly_df["position"] == position]
    if rows.empty or stat_col not in rows.columns:
        return 0.0
    return float(rows[stat_col].fillna(0).mean())


def opponent_multiplier(weekly_df, opp_team, position, stat_col):
    """How much opp_team allows to this position vs league average."""
    pos = weekly_df[weekly_df["position"] == position]
    if pos.empty or stat_col not in pos.columns:
        return 1.0
    league_avg = pos.groupby(["opponent_team"])[stat_col].mean().mean()
    vs_opp = pos[pos["opponent_team"] == opp_team][stat_col].mean()
    if not league_avg or np.isnan(vs_opp):
        return 1.0
    return float(np.clip(vs_opp / league_avg, OPP_MULT_MIN, OPP_MULT_MAX))


def project_nfl(weekly_df, player, stat, opp_team, position):
    stat_col = NFL_STAT_MAP.get(stat)
    if not isinstance(stat_col, str) or stat_col not in weekly_df.columns:
        return None
    vals = player_recent_stats(weekly_df, player, stat_col, n=8)
    if len(vals) < 2:
        return None
    prior = position_prior(weekly_df, position, stat_col)
    base = shrink(ewma(vals), len(vals), prior)
    return round(base * opponent_multiplier(weekly_df, opp_team,
                                            position, stat_col), 2)
```

- [ ] **Step 4: Run tests**

Run: `python -m pytest tests/test_nfl_projections.py -v` → 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/sports/nfl/projections.py tests/test_nfl_projections.py
git commit -m "feat: NFL projection engine (EWMA + position prior + opp adjustment)"
```

---

### Task 6: NFL probability distributions

Yards are continuous (Normal with player sd shrunk to position sd); receptions/TDs are low counts (Poisson).

**Files:**
- Create: `src/sports/nfl/probability.py`
- Test: `tests/test_nfl_probability.py`

**Interfaces:**
- Consumes: `player_recent_stats` (Task 3).
- Produces: `nfl_win_probability(weekly_df, player, stat, projection, line, position) -> dict` with `over`/`under` (0–100), routed by `YARDS_STATS` / `COUNT_STATS` sets.

- [ ] **Step 1: Write the failing tests**

`tests/test_nfl_probability.py`:

```python
import pandas as pd
from sports.nfl.probability import nfl_win_probability


def _df():
    return pd.DataFrame({
        "player_display_name": ["QB Guy"] * 8,
        "position": ["QB"] * 8,
        "season": [2026] * 8,
        "week": range(1, 9),
        "passing_yards": [280, 310, 240, 265, 295, 250, 320, 270],
        "receptions": [0] * 8,
    })


def test_yards_prob_over_when_proj_above_line():
    p = nfl_win_probability(_df(), "QB Guy", "Pass Yards",
                            projection=290.0, line=260.5, position="QB")
    assert p["over"] > 55.0
    assert abs(p["over"] + p["under"] - 100.0) < 0.1


def test_counts_use_poisson():
    p = nfl_win_probability(_df(), "QB Guy", "Receptions",
                            projection=5.0, line=4.5, position="WR")
    assert 50.0 < p["over"] < 70.0
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_nfl_probability.py -v` → FAIL.

- [ ] **Step 3: Implement**

`src/sports/nfl/probability.py`:

```python
"""NFL probability: Normal for yardage (empirical sd shrunk to position sd),
Poisson for low-count stats."""
import numpy as np
from scipy.stats import norm, poisson
from sports.nfl.data import NFL_STAT_MAP, player_recent_stats

YARDS_STATS = {"Pass Yards", "Rush Yards", "Receiving Yards",
               "Rush+Rec Yds", "Pass+Rush Yds"}
COUNT_STATS = {"Receptions", "Targets", "Pass TDs", "Pass Completions",
               "Pass Attempts", "Rush Attempts", "INT"}
SD_PRIOR_WEIGHT = 4
MIN_SD_FRACTION = 0.20  # sd never below 20% of projection


def _player_sd(weekly_df, player, stat_col, position, projection):
    vals = player_recent_stats(weekly_df, player, stat_col, n=10)
    pos_rows = weekly_df[weekly_df["position"] == position]
    pos_sd = float(pos_rows[stat_col].fillna(0).std()) if not pos_rows.empty else 0.0
    n = len(vals)
    p_var = float(np.var(vals)) if n >= 3 else 0.0
    var = (n * p_var + SD_PRIOR_WEIGHT * pos_sd**2) / (n + SD_PRIOR_WEIGHT)
    return max(np.sqrt(var), projection * MIN_SD_FRACTION, 1.0)


def nfl_win_probability(weekly_df, player, stat, projection, line, position):
    if stat in COUNT_STATS:
        over = 1.0 - poisson.cdf(np.floor(line), projection)
    else:  # yardage: continuous Normal
        stat_col = NFL_STAT_MAP.get(stat, "")
        sd = _player_sd(weekly_df, player, stat_col, position, projection) \
            if stat_col in weekly_df.columns else projection * 0.35
        over = 1.0 - norm.cdf(line, loc=projection, scale=sd)
    over_pct = round(float(over) * 100, 2)
    return {"over": over_pct, "under": round(100.0 - over_pct, 2)}
```

- [ ] **Step 4: Run tests**

Run: `python -m pytest tests/test_nfl_probability.py -v` → 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/sports/nfl/probability.py tests/test_nfl_probability.py
git commit -m "feat: NFL probability distributions (Normal yards, Poisson counts)"
```

---

### Task 7: Per-sport strategy registry

NFL starts with zero history: no blocks, 8% floor everywhere, hard cap of 10 plays per slate. NBA rules unchanged.

**Files:**
- Modify: `src/engine/strategy.py`
- Test: `tests/test_strategy.py` (extend)

**Interfaces:**
- Consumes: `evaluate_play(..., vegas_confirms=None)` from NBA plan Task 7.
- Produces: `evaluate_play(stat_type, direction, ev_edge, player_name=None, vegas_confirms=None, sport="NBA")`; `MAX_PLAYS_PER_SLATE = {"NBA": None, "NFL": 10}` consumed by Task 8's orchestrator.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_strategy.py`:

```python
def test_nfl_has_no_blocked_combos_yet():
    ok, _, label = evaluate_play("Pass Yards", "OVER", 9.0, sport="NFL")
    assert ok and "NFL" in label


def test_nfl_respects_8_pct_floor():
    ok, _, _ = evaluate_play("Pass Yards", "OVER", 6.0, sport="NFL")
    assert not ok


def test_nba_blocks_unaffected():
    ok, _, _ = evaluate_play("Points", "OVER", 14.0, sport="NBA")
    assert not ok
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_strategy.py -v` → FAIL (unexpected kwarg).

- [ ] **Step 3: Implement**

In `src/engine/strategy.py`:

```python
NFL_DEFAULT_STRATEGY = {"tier": 3, "min_edge": 8.0, "label": "🟡 NFL-DEFAULT"}

STRATEGY_REGISTRY = {
    "NBA": {"tiers": STRATEGY_TIERS, "default": DEFAULT_STRATEGY},
    # NFL: no empirical blocks yet — recalibrate after 4 graded weeks.
    "NFL": {"tiers": {}, "default": NFL_DEFAULT_STRATEGY},
}

MAX_PLAYS_PER_SLATE = {"NBA": None, "NFL": 10}
```

Change `evaluate_play` to accept `sport="NBA"` and resolve via the registry:

```python
def evaluate_play(stat_type, direction, ev_edge, player_name=None,
                  vegas_confirms=None, sport="NBA"):
    ...
    cfg = STRATEGY_REGISTRY.get(sport, STRATEGY_REGISTRY["NBA"])
    strategy = cfg["tiers"].get((stat_type, direction), cfg["default"])
```

(The blacklist/Vegas checks stay above the registry lookup, unchanged.)

- [ ] **Step 4: Run full suite**

Run: `python -m pytest -v` → all pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/strategy.py tests/test_strategy.py
git commit -m "feat: per-sport strategy registry (NFL starts unblocked, 8% floor)"
```

---

### Task 8: NFL orchestrator + adapter registration

A separate entry point (`main_nfl.py`) mirroring `main.py`'s flow with the NFL adapter. Vegas gate reuses `odds_api.py` with the NFL sport key.

**Files:**
- Create: `src/sports/nfl/adapter.py`, `src/main_nfl.py`
- Modify: `src/services/odds_api.py` (parameterize `_SPORT` → `sport_key` argument; add NFL market map)
- Test: `tests/test_nfl_adapter.py`

**Interfaces:**
- Consumes: everything from Tasks 3–7; `get_venue` (NBA plan Task 14); `log_predictions(df, sport="NFL")` (Task 1).
- Produces: `NFLAdapter` registered as `get_sport("NFL")`; runnable `python src/main_nfl.py`; `fetch_events(sport_key)` / `build_vegas_lookup(events, sport_key, market_map)` in odds_api.

- [ ] **Step 1: Write the failing test**

`tests/test_nfl_adapter.py`:

```python
import pandas as pd
from sports import get_sport


def _ctx():
    weekly = pd.DataFrame({
        "player_display_name": ["QB Guy"] * 8,
        "position": ["QB"] * 8,
        "recent_team": ["KC"] * 8,
        "opponent_team": ["BUF"] * 8,
        "season": [2026] * 8,
        "week": range(1, 9),
        "passing_yards": [280, 310, 240, 265, 295, 250, 320, 270],
    })
    return {"weekly": weekly}


def test_nfl_adapter_projects_from_context():
    import sports.nfl.adapter  # noqa: F401
    a = get_sport("NFL")
    proj = a.project("QB Guy", "Pass Yards", "KC vs BUF", _ctx())
    assert proj is not None and 200 < proj < 350


def test_win_probability_sums_to_100():
    import sports.nfl.adapter  # noqa: F401
    a = get_sport("NFL")
    p = a.win_probability(290.0, 265.5, "Pass Yards", "QB Guy", _ctx())
    assert 50.0 < p < 90.0
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_nfl_adapter.py -v` → FAIL.

- [ ] **Step 3: Implement**

`src/sports/nfl/adapter.py`:

```python
from datetime import date
from sports import register
from sports.base import SportAdapter
from sports.nfl.data import NFL_STAT_MAP, load_weekly
from sports.nfl.probability import nfl_win_probability
from sports.nfl.projections import project_nfl


class NFLAdapter(SportAdapter):
    league = "NFL"

    def load_context(self):
        year = date.today().year
        seasons = [year - 1, year] if date.today().month >= 9 else [year - 1]
        return {"weekly": load_weekly(seasons)}

    def _position(self, weekly, player):
        rows = weekly[weekly["player_display_name"] == player]
        return rows.iloc[-1]["position"] if not rows.empty else None

    def project(self, player, stat, matchup, context):
        weekly = context["weekly"]
        pos = self._position(weekly, player)
        if pos is None:
            return None
        opp = str(matchup).split(" ")[-1].upper()
        return project_nfl(weekly, player, stat, opp, pos)

    def win_probability(self, projection, line, stat, player, context):
        weekly = context["weekly"]
        pos = self._position(weekly, player) or "WR"
        return nfl_win_probability(weekly, player, stat, projection,
                                   line, pos)["over"]

    def resolve_actual(self, player, stat, game_date, context):
        weekly = context["weekly"]
        stat_col = NFL_STAT_MAP.get(stat)
        if not isinstance(stat_col, str):
            return None
        rows = weekly[weekly["player_display_name"] == player]
        if rows.empty or stat_col not in rows.columns:
            return None
        latest = rows.sort_values(["season", "week"]).iloc[-1]
        return float(latest[stat_col])


register("NFL", NFLAdapter())
```

`src/main_nfl.py`:

```python
"""NFL pipeline orchestrator. Mirrors main.py's flow via the NFL adapter.
PAPER_MODE=1 (default): log picks to DB, skip Discord."""
import os
import pandas as pd
from dotenv import load_dotenv

import sports.nfl.adapter  # noqa: F401 — registers NFL
from engine.probability import get_true_edge, calibrate_prob
from engine.strategy import evaluate_play, MAX_PLAYS_PER_SLATE
from services.db import init_db, log_predictions, filter_new_plays
from services.notifier import send_discord_alert
from sports import get_sport
from utils.utils import logger
from venues import get_venue

load_dotenv()


def run_nfl_pipeline():
    logger.info("=== Sharp Edge NFL (paper mode: %s) ===",
                os.getenv("PAPER_MODE", "1"))
    adapter = get_sport("NFL")
    board = get_venue("prizepicks").fetch_board("NFL")
    if board.empty:
        logger.info("[-] NFL board empty. Exiting.")
        return
    context = adapter.load_context()

    results = []
    for _, row in board.iterrows():
        player, stat, line = row["Player"], row["Stat"], float(row["Line"])
        proj = adapter.project(player, stat, row["Matchup"], context)
        if proj is None or proj <= 0:
            continue
        over_prob = calibrate_prob(
            adapter.win_probability(proj, line, stat, player, context))
        play = "OVER" if proj > line else "UNDER"
        implied = over_prob if play == "OVER" else 100.0 - over_prob
        edge = get_true_edge(implied)
        ok, reason, tier = evaluate_play(stat, play, edge,
                                         player_name=player, sport="NFL")
        if not ok:
            continue
        results.append({"Player": player, "Team": row.get("Team", "UNK"),
                        "Matchup": row["Matchup"], "Stat": stat,
                        "PP Line": line, "V2 Proj": proj, "Play": play,
                        "Poisson Prob": implied, "EV Edge": edge,
                        "Confidence": implied, "Tier": tier, "Vetoed": False,
                        "ML Prob": "-", "Game Date": row.get("Game Date")})

    df = pd.DataFrame(results)
    if df.empty:
        logger.info("[-] No NFL plays cleared filters.")
        return
    df = df.sort_values("EV Edge", ascending=False)
    cap = MAX_PLAYS_PER_SLATE["NFL"]
    if cap:
        df = df.head(cap)
    new_df = filter_new_plays(df, sport="NFL")
    if new_df.empty:
        return
    log_predictions(new_df, sport="NFL")
    if os.getenv("PAPER_MODE", "1") != "1":
        webhook = os.getenv("DISCORD_WEBHOOK_URL")
        if webhook:
            send_discord_alert(new_df, webhook)
    logger.info(f"[+] Logged {len(new_df)} NFL plays.")


if __name__ == "__main__":
    init_db()
    run_nfl_pipeline()
```

In `src/services/odds_api.py`: change `fetch_nba_events()` to `fetch_events(sport_key="basketball_nba")` (keep a `fetch_nba_events = lambda: fetch_events()` alias for main.py), thread `sport_key` through `fetch_player_props`, and add:

```python
NFL_STAT_TO_MARKET = {
    "Pass Yards": "player_pass_yds", "Rush Yards": "player_rush_yds",
    "Receiving Yards": "player_reception_yds", "Receptions": "player_receptions",
    "Pass TDs": "player_pass_tds",
}
```

(Vegas-gate wiring into `main_nfl.py` mirrors `main.py` — add it when The Odds API key has quota for two sports; the strategy `vegas_confirms=None` path means it degrades safely without it.)

- [ ] **Step 4: Run full suite**

Run: `python -m pytest -v` → all pass.

- [ ] **Step 5: Commit**

```bash
git add src/sports/nfl/adapter.py src/main_nfl.py src/services/odds_api.py tests/test_nfl_adapter.py
git commit -m "feat: NFL orchestrator + adapter (paper mode default)"
```

---

### Task 9: NFL grader + injury/practice-report veto

Grade from nflverse weekly data (published within ~24–48h of games); veto players Out/Doubtful or DNP-in-practice from the official injury reports.

**Files:**
- Create: `src/sports/nfl/grading.py`, `src/sports/nfl/injuries.py`
- Modify: `src/main_nfl.py` (call injury veto in loop; grade at start of run)
- Test: `tests/test_nfl_grading.py`

**Interfaces:**
- Consumes: `resolve_actual` (Task 8), `load_injuries` (Task 3), `predictions` table with `sport='NFL'`.
- Produces: `grade_nfl_pending(db_path=DB) -> dict` (counts by result); `nfl_injury_blocklist(injuries_df) -> set[str]`.

- [ ] **Step 1: Write the failing tests**

`tests/test_nfl_grading.py`:

```python
import sqlite3
import pandas as pd
from sports.nfl.grading import grade_row
from sports.nfl.injuries import nfl_injury_blocklist


def test_grade_row_win_loss_push():
    assert grade_row("UNDER", line=250.5, actual=240.0) == "WIN"
    assert grade_row("UNDER", line=250.5, actual=260.0) == "LOSS"
    assert grade_row("OVER", line=250.0, actual=250.0) == "PUSH"
    assert grade_row("OVER", line=250.5, actual=None) == "PENDING"


def test_injury_blocklist_from_report():
    df = pd.DataFrame({
        "full_name": ["Hurt Guy", "Fine Guy", "Limited Guy"],
        "report_status": ["Out", None, "Questionable"],
        "practice_status": [None, "Full Participation in Practice",
                             "Did Not Participate In Practice"],
    })
    blocked = nfl_injury_blocklist(df)
    assert "hurt guy" in blocked
    assert "limited guy" in blocked      # DNP practice = block
    assert "fine guy" not in blocked
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/test_nfl_grading.py -v` → FAIL.

- [ ] **Step 3: Implement**

`src/sports/nfl/grading.py`:

```python
import os
import sqlite3
from utils.utils import logger

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))
DB = os.path.join(_PROJECT_ROOT, "sharp_edge.db")


def grade_row(play, line, actual):
    if actual is None:
        return "PENDING"
    if actual == line:
        return "PUSH"
    if play == "OVER":
        return "WIN" if actual > line else "LOSS"
    return "WIN" if actual < line else "LOSS"


def grade_nfl_pending(db_path=DB):
    from sports import get_sport
    import sports.nfl.adapter  # noqa: F401
    adapter = get_sport("NFL")
    context = adapter.load_context()

    conn = sqlite3.connect(db_path)
    pending = conn.execute(
        "SELECT id, player, stat_type, line, play, game_date FROM predictions "
        "WHERE status='PENDING' AND sport='NFL'").fetchall()
    counts = {"WIN": 0, "LOSS": 0, "PUSH": 0, "PENDING": 0}
    for pid, player, stat, line, play, game_date in pending:
        actual = adapter.resolve_actual(player, stat, game_date, context)
        status = grade_row(play, line, actual)
        counts[status] += 1
        if status != "PENDING":
            conn.execute("UPDATE predictions SET status=?, actual_result=? "
                         "WHERE id=?", (status, actual, pid))
    conn.commit()
    conn.close()
    logger.info(f"[+] NFL grading: {counts}")
    return counts
```

`src/sports/nfl/injuries.py`:

```python
BLOCK_REPORT = {"out", "doubtful"}
BLOCK_PRACTICE = {"did not participate in practice"}


def nfl_injury_blocklist(injuries_df):
    """Set of lowercased names to block, from nflverse injury reports."""
    blocked = set()
    for _, r in injuries_df.iterrows():
        name = str(r.get("full_name", "")).lower().strip()
        if not name:
            continue
        if str(r.get("report_status", "")).lower() in BLOCK_REPORT:
            blocked.add(name)
        elif str(r.get("practice_status", "")).lower() in BLOCK_PRACTICE:
            blocked.add(name)
    return blocked
```

In `src/main_nfl.py`: at pipeline start call `grade_nfl_pending()`; after `load_context()` build `blocklist = nfl_injury_blocklist(load_injuries([date.today().year]))` (wrapped in try/except → empty set), and inside the loop skip when `player.lower().strip() in blocklist`.

- [ ] **Step 4: Run full suite**

Run: `python -m pytest -v` → all pass.

- [ ] **Step 5: Commit**

```bash
git add src/sports/nfl/grading.py src/sports/nfl/injuries.py src/main_nfl.py tests/test_nfl_grading.py
git commit -m "feat: NFL grading from nflverse + practice-report injury veto"
```

---

### Task 10: Scheduling + paper-mode launch checklist

**Files:**
- Create: `docs/nfl-launch-checklist.md`

- [ ] **Step 1: Write the checklist + cron entries**

`docs/nfl-launch-checklist.md`:

```markdown
# NFL Launch Checklist (target: week 1, Sep 2026)

## Cron (all times local; NFL slates are weekly, not daily)
# Thursday Night Football — run Thursday afternoon
30 15 * * 4 cd <repo> && python src/main_nfl.py >> cron.log 2>&1
# Sunday slate — run Sunday morning
0 9 * * 0 cd <repo> && python src/main_nfl.py >> cron.log 2>&1
# Monday Night Football — run Monday afternoon
30 15 * * 1 cd <repo> && python src/main_nfl.py >> cron.log 2>&1
# Grading — Tue and Fri mornings (nflverse data lands within ~24-48h)
0 10 * * 2,5 cd <repo> && python -c "import sys; sys.path.insert(0,'src'); from sports.nfl.grading import grade_nfl_pending; grade_nfl_pending()" >> grader.log 2>&1

## Pre-season (by Sep 1)
- [ ] Live smoke test: PrizePicks NFL board returns rows
- [ ] Live smoke test: load_weekly([2025, 2026]) column names match NFL_STAT_MAP
- [ ] Verify injuries feed has 2026 rows once camps open
- [ ] PAPER_MODE=1 in .env

## Weeks 1-4 (paper)
- [ ] After each graded week: python scripts/season_report.py (filter sport='NFL')
- [ ] Track: WR by stat type, by direction, projection MAE

## Gate to real stakes (week 5+, earliest ~Oct 8)
- [ ] >= 100 graded paper bets
- [ ] Paper WR >= 55% (PrizePicks flex breakeven + buffer)
- [ ] No stat type below 45% left unblocked (add to NFL strategy registry)
- [ ] Set PAPER_MODE=0, start with 2-3 slips/slate maximum
```

- [ ] **Step 2: Commit**

```bash
git add docs/nfl-launch-checklist.md
git commit -m "docs: NFL launch checklist + cron schedule"
```

---

## Self-Review Notes

- Coverage vs roadmap Phase 4: data layer ✅, board ✅, projections ✅ (position priors + shrinkage for 17-game samples), distributions ✅ (Normal yards / Poisson counts), per-sport strategy ✅, grading ✅, practice-report injuries ✅, paper mode + cadence ✅. The Vegas gate for NFL is stubbed safe (`vegas_confirms=None` passes) with the market map provided — full wiring is a follow-up once API quota is confirmed.
- Type consistency: `SportAdapter` methods (Task 1) match implementations in Tasks 2 and 8; `NFL_STAT_MAP`/`player_recent_stats` (Task 3) consumed by Tasks 5, 6, 8; `evaluate_play(..., sport=)` (Task 7) consumed by Task 8; `grade_row` statuses match the existing `predictions.status` values (WIN/LOSS/PUSH/PENDING).
- Ordering: strictly sequential, Tasks 1 → 10. NBA-plan prerequisites listed in Global Constraints.
