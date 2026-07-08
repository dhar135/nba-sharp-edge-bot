import numpy as np
import joblib
import engine.probability as prob


def test_identity_when_no_model(monkeypatch, tmp_path):
    monkeypatch.setattr(prob, "_CALIBRATION_PATH", str(tmp_path / "none.pkl"))
    monkeypatch.setattr(prob, "_CALIBRATOR", None)
    monkeypatch.setattr(prob, "_CALIBRATOR_LOADED", False)
    assert prob.calibrate_prob(63.0) == 63.0


def test_corrupt_calibrator_file_falls_back_to_identity(monkeypatch, tmp_path):
    """A corrupt/incompatible models/calibration.pkl must not crash the pipeline —
    calibrate_prob should log a warning and behave as identity (no calibrator)."""
    path = tmp_path / "corrupt.pkl"
    path.write_bytes(b"not a pickle")
    monkeypatch.setattr(prob, "_CALIBRATION_PATH", str(path))
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
