# Sharp Edge — Full Season Review & Improvement Roadmap
> **Date:** July 5, 2026
> **Data reviewed:** `sharp_edge.db` (Apr 27 – Jun 13, 2026), V2.1 codebase, V2.1 performance report

---

## Part 1: What the season actually produced

### Finding #1 (critical): Your results are double-counted — real sample is 1,665 bets, not 3,820

The dedup key in `src/services/db.py` is `(date, player, stat_type, game_date)` — where `date` is the **run date**, not the game date. When the pipeline runs on Apr 26 *and* Apr 27 for an Apr 27 game, the same play is inserted twice and graded twice. 1,195 play-groups have duplicate graded rows (2,952 of 3,820 graded rows belong to duplicated groups).

**Deduplicated truth:**

| Metric | Reported | Actual (unique bets) |
|---|---|---|
| Graded bets | 3,820 | **1,665** |
| Win rate | 58.8% | **60.2%** |

Good news: WR is actually slightly *better* deduped. Bad news: every sample size in every prior report is ~2.3x inflated, so several "statistically significant" conclusions (blacklist entries, stat blocks) were drawn on half the data you thought you had.

### Finding #2 (critical): Performance is decaying month over month, and June was unprofitable

| Month | Unique bets | Win rate |
|---|---|---|
| April | 332 | 66.6% |
| May | 1,183 | 58.5% |
| June (playoffs) | 150 | **52.0%** |

June is below even the optimistic 52.4% breakeven. The UNDER-heavy system (93% of volume) got crushed by playoff dynamics: rotations shrink to 8 players, starters play 40+ minutes, and every remaining player's volume goes UP. June UNDERs: Points 45.9%, Pts+Rebs 45.8%, Rebounds 41.3%, Rebs+Asts 46.3%. The engine has no playoff mode — it just re-uses regular-season logic with tiny playoff samples.

### Finding #3 (critical): The breakeven target itself is wrong

The code hardcodes `sportsbook_implied=54.2` and reports cite 52.4% breakeven — that's the breakeven for a **-110 sportsbook bet**. PrizePicks pays fixed ladders:

- 2-pick power (3x): per-leg breakeven **57.7%**
- 3-pick power (5x): per-leg breakeven **58.5%**
- 3-pick flex / 5-pick flex: roughly **54.5–57%** per leg depending on ladder

At 60.2% per-leg the season was genuinely profitable, but the true margin is ~2–5 points, not the "+8.4 points" reported — and June (52.0%) lost real money at any PrizePicks payout structure.

### Finding #4 (high): The Vegas comparison revealed where the edge actually lives — and it's being ignored

Deduplicated results by Vegas coverage:

| Vegas data | Bets | Win rate |
|---|---|---|
| No major-book line exists | 1,542 | **60.4%** |
| Vegas confirms the pick | 107 | 50.5% |
| Vegas diverges | 16 | **37.5%** |

When DraftKings/FanDuel/BetMGM bother to price the prop, your win rate collapses to roughly coin-flip. Your entire edge is in **illiquid props** — role players and obscure markets where PrizePicks lines are soft. This is the single most strategically important fact in the database, and today it only produces a log line and a Discord emoji. Vegas divergence should be a hard veto, and Vegas coverage itself should factor into confidence.

### Finding #5 (high): The tier system has been inverted all season

| Tier | Min edge | Unique bets | Win rate |
|---|---|---|---|
| 🟡 CAUTIOUS | 8.0% | 176 | 65.9% |
| 🟡 STANDARD | 5.0% | 582 | 63.1% |
| 🟢 ELITE | 2.0% | 174 | 57.5% |
| 🟡 DEFAULT | 5.0% | 148 | 57.4% |
| 🟢 STRONG | 3.0% | 585 | **55.2%** |

The tiers with the *lowest* edge thresholds perform worst, because they admit 2.5–8% edge plays, which run at 50.5–52.2% (below breakeven). STRONG is your **largest volume bucket** and is barely breakeven. The May 13 report flagged this ("tier inversion persists") and the recalibration didn't fix it — because the labels were recalibrated but the thresholds still let low-edge plays through. A global edge floor of ~8% would have removed ~745 near-breakeven bets and raised the season WR meaningfully.

### Finding #6 (high): Probability calibration is broken — 50% of all bets hit the 15% edge cap

1,910 of 3,820 graded rows have `ev_edge = 15.0` exactly. Half the board triggers a raw edge ≥15%, i.e., the NegBin/Poisson model claims ≥69% win probability on plays that realize ~62%. The distributions understate variance (no minutes uncertainty, no game-script/blowout variance, no opponent-specific variance), producing phantom edges. The cap is a band-aid that hides the miscalibration instead of fixing it. You cannot do payout-aware EV or Kelly staking with probabilities this miscalibrated.

### Finding #7 (medium): Strategy changes ship too slowly, and the blacklist is reactive

- The De'Aaron Fox + Devin Vassell blacklist additions are **still uncommitted** in the working tree. Fox went **0-for-25 after May 13** while the fix sat undeployed.
- Turnovers UNDER bets were still logged May 15 — two days after the block was "added."
- The blacklist itself is overfit: Chet Holmgren at 0/6 is noise, not signal. Meanwhile Duncan Robinson went 5/5 *after* being blacklisted. Name lists treat the symptom; the disease is the model missing **role changes** (trades, rotation changes, usage shifts). A role-change detector (EWMA minutes/usage vs season baseline) would generalize; a name list won't.

### Finding #8 (medium): Operational gaps

- **141 VOID (DNP) bets** — no injury/lineup feed. You're alerting picks on players who don't suit up.
- **No ROI tracking** — only win rate. A 60% WR with all volume in the thinnest tier can lose money; you can't see that today.
- **No CLV (closing line value) logging** — the fastest way to know if edge is real before results accumulate.
- **Regular-season boot bug:** `main.py` hardcodes `season_type="Playoffs"` for `adv_recent` and exits if it's empty — next October the pipeline will fail on day one.
- **Veto layer makes one `playergamelog` API call per prop** with 0.3s sleeps — the exact per-player pattern the "God-Call" design was meant to eliminate; slow and rate-limit prone.
- **No backtest harness** — every strategy change is A/B tested with real money in production.
- Confidence score is ad-hoc weights (0.35/0.35/0.30) and empirically weakly predictive; it's used as the sort order for alerts.

---

## Part 2: Improvement plan

### Phase 0 — Data integrity & hygiene (do first, ~1 day)
1. Fix the dedup key to `(game_date, player, stat_type)` in `db.py` (both `log_predictions` and `filter_new_plays`); add a UNIQUE index.
2. One-time DB cleanup script: collapse the 1,195 duplicate groups (keep earliest row).
3. Commit the working-tree changes (blacklist, Vegas integration) — deployed code should match repo.
4. Regenerate the performance report from deduplicated data so future decisions use real sample sizes.

### Phase 1 — Stop the bleeding (before next season, ~1 week)
5. **Global edge floor at 8%** (or restructure tiers so no tier admits <8%). This is the single highest-EV change: it removes the 50–52% WR volume.
6. **Vegas divergence = hard block; Vegas coverage = confidence penalty.** Lean into the illiquid-market niche deliberately.
7. **Playoff mode:** either shut off during playoffs or build separate playoff logic (rotation-aware minutes, series context, 8-man rotations). June proved the current engine is unprofitable in playoffs.
8. **Injury/lineup feed** (e.g., NBA injury report scrape or a rotowire-style source) to kill DNP voids and late-scratch mis-projections.
9. Replace the player blacklist with a **role-change detector**: flag players whose recent minutes/usage EWMA deviates >20% from season baseline, and veto or widen uncertainty instead of banning names.
10. Fix the `season_type="Playoffs"` hardcode with automatic season-phase detection.

### Phase 2 — Model quality (off-season project, ~2–4 weeks)
11. **Recalibrate probabilities:** bucket predicted probability vs. realized frequency from the 1,665-bet sample and fit a calibration curve (isotonic/Platt). Target: predicted 60% ≈ realized 60%. Then remove or raise the 15% cap — it should almost never trigger.
12. **Model minutes explicitly** (the dominant variance source): distribution over minutes (injury risk, blowout, rotation) rather than point estimate × multiplier.
13. **Payout-aware EV:** compute EV per PrizePicks ladder (2-power, 3-flex, etc.) instead of the hardcoded 54.2% strawman; recommend slip construction, not just legs.
14. **Backtest harness:** replay any strategy config against the historical DB before deploying. No more live-money A/B tests.
15. Track **ROI and CLV** per bet alongside WR.

### Phase 3 — Architecture for multi-sport / multi-venue (off-season, ~2–3 weeks)
16. Split into sport-agnostic core (pipeline, strategy filter, probability, DB, notifier, grader interfaces) + sport adapters (`nba/`, `nfl/`) + venue adapters (`prizepicks/`, `kalshi/`).
17. Config-driven stat mappings, season calendars, and payout structures per venue.

### Phase 4 — NFL launch (Aug–Sep 2026)
18. NFL adapter: `nfl_data_py`/nflverse for stats (free, well-maintained), PrizePicks NFL board (same API, different league filter), The Odds API supports NFL props.
19. Model differences to respect: 17-game samples (EWMA over 20 games doesn't exist — use position/opponent priors + hierarchical shrinkage), yards ≈ continuous (gamma/normal, not Poisson), TDs ≈ low-count Poisson, game script and Vegas totals/spreads matter far more than pace.
20. Exploit the NFL's formalized Wed–Fri practice/injury reports — a real informational edge unavailable in NBA.
21. Paper-trade weeks 1–4 before real volume; NFL gives only ~18 feedback cycles per year, so stakes must start small.

---

## Part 3: The Kalshi question

**Recommendation: don't switch — keep PrizePicks as the primary venue, add Kalshi as a second venue behind an abstraction, and be skeptical of team picks.**

Why not a switchover:
- **Your measured edge is in soft, illiquid lines.** Props with no major-book coverage hit 60.4%; props Vegas prices hit ~49%. Kalshi is an exchange with an order book — prices come from other traders, and the liquid markets there (star players, game winners) are exactly the segment where this system has no demonstrated edge.
- **Team picks (game winners/spreads) on Kalshi are efficiently priced** — they get arbitraged against sportsbook lines. Beating them requires a genuine team-level model with closing-line value, which is a different and harder project than player props, with thinner margins. Nothing in the current codebase transfers to that problem. "More profitable" is unlikely; the per-player mispricing you exploit doesn't exist at team level.

Why add Kalshi anyway:
- **PrizePicks bans/limits winners.** At 60% WR with growing volume, account risk is your biggest long-term threat. Kalshi is a regulated exchange — winning is the business model, not a ToS violation.
- **Real API** (documented REST + WebSocket, API-key auth, demo environment) vs. cloudscraper-ing an unofficial PrizePicks endpoint that can break or block you any day.
- **You can exit positions** mid-game and get true market prices (real probabilities to calibrate against).
- Kalshi has been expanding sports/player-prop markets rapidly; where their player markets overlap your illiquid-prop niche, the same projections apply. (Verify their current market catalog and fee schedule — it changes fast.)

Practical path: Phase 3's venue abstraction, then paper-trade your existing NBA projections against Kalshi player markets for a month and measure realized edge net of fees before committing real bankroll.

---

## Bottom line

The system is genuinely profitable (60.2% over 1,665 unique bets) but three things are eroding it: duplicate-inflated reporting hiding true sample sizes, a tier system that lets ~45% of volume run at breakeven, and a playoff regime it doesn't understand. The Vegas comparison you just built is the most valuable diagnostic in the codebase — it shows the edge is an *illiquid-market* edge. Protect that niche, cut the low-edge volume, fix calibration, and the same core engine ports to NFL with a sport-adapter refactor over the off-season.
