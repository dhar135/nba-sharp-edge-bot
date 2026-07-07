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
