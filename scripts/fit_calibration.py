"""Fit isotonic calibration: raw model probability -> realized win frequency.
Prints a reliability table, saves models/calibration.pkl."""
import os
import sqlite3
import joblib
import numpy as np
from sklearn.isotonic import IsotonicRegression

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Avoid calibration circularity: main.py logs the CALIBRATED probability into
# poisson_prob, but we must always fit on RAW (pre-calibration) probabilities —
# otherwise re-fitting trains on its own previous correction and compounds it.
#
# Historical pre-calibration rows have no raw_prob (it's NULL) and their raw value
# lives in poisson_prob. Post-calibration rows carry the raw value in raw_prob, while
# poisson_prob holds the already-calibrated value. COALESCE(raw_prob, poisson_prob)
# therefore always yields the raw probability, never a calibrated one.
rows = sqlite3.connect(os.path.join(ROOT, "sharp_edge.db")).execute(
    "SELECT COALESCE(raw_prob, poisson_prob), status='WIN' FROM predictions "
    "WHERE status IN ('WIN','LOSS') AND COALESCE(raw_prob, poisson_prob) IS NOT NULL").fetchall()
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
