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


def test_vegas_divergence_blocks():
    ok, reason, label = evaluate_play("Rebounds", "UNDER", 10.0, vegas_confirms=False)
    assert not ok and "VEGAS" in reason


def test_vegas_confirming_line_also_blocks():
    # 2026-07: presence of any major-book line is the negative signal.
    # Confirming picks won only 50.9% (n=106) — below breakeven.
    ok, reason, _ = evaluate_play("Rebounds", "UNDER", 10.0, vegas_confirms=True)
    assert not ok and "VEGAS" in reason


def test_unpriced_prop_passes():
    # No major-book line (vegas_confirms=None) is the profitable niche: 65.2%.
    assert evaluate_play("Rebounds", "UNDER", 10.0, vegas_confirms=None)[0]
