import json
import math
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from pymetropolis.metro_calibration.survey.joint_travel import (
    EstimateJointToursClassifierStep,
    ExternalJointToursClassifierStep,
)
from pymetropolis.metro_calibration.survey.modes import (
    EstimateModeClassifierStep,
    ExternalModeClassifierStep,
)
from pymetropolis.metro_common import ml_models
from pymetropolis.metro_pipeline import Config
from pymetropolis.metro_pipeline.graph import least_priority_steps

SEED = 1907

FEATURES = ["signal", "noise"]


def get_data():
    """Returns a small dataset where `signal` is correlated with the target, with observations
    grouped by pairs (as the tours of a household are)."""
    rng = np.random.default_rng(SEED)
    n = 200
    signal = rng.normal(size=n)
    y = pd.Series((signal + rng.normal(scale=0.5, size=n) > 0).astype(int))
    X = pd.DataFrame({"signal": signal, "noise": rng.normal(size=n)})
    groups = np.repeat(np.arange(n // 2), 2)
    return X, y, groups


def check_scores(scores: dict):
    assert set(scores) == {"brier", "fidelity"}
    for value in scores.values():
        assert math.isfinite(value)
        assert value >= 0


def test_results_with_user_model():
    X, y, groups = get_data()
    estimator, estimation = ml_models.estimate_model(X, y, groups, "logistic", SEED, 1)
    results = ml_models.estimation_results(y, groups, FEATURES, "logistic", None, estimation, SEED)
    # The results must be JSON-serializable.
    results = json.loads(json.dumps(results))
    assert results["model"] == "logistic"
    assert results["model_selection"] == "user"
    assert "model_comparison" not in results
    assert results["features"] == FEATURES
    assert results["sample"]["nb_observations"] == len(y)
    assert results["sample"]["nb_groups"] == len(y) // 2
    assert set(results["sample"]["class_shares"]) == {"0", "1"}
    assert math.isclose(sum(results["sample"]["class_shares"].values()), 1.0)
    assert results["random_seed"] == SEED
    assert results["cv_folds"] == 5
    assert results["nb_candidates"] == 7
    assert set(results["best_params"]) == {"C"}
    check_scores(results["scores"])
    assert hasattr(estimator, "predict_proba")


def test_results_with_model_comparison():
    X, y, groups = get_data()
    model, comparison = ml_models.test_models(X, y, groups, SEED, 1)
    assert "dummy" in comparison
    for scores in comparison.values():
        check_scores(scores)
    # The best model is the one with the lowest Brier score.
    assert model == min(comparison, key=lambda m: comparison[m]["brier"])
    _, estimation = ml_models.estimate_model(X, y, groups, "dummy", SEED, 1)
    results = ml_models.estimation_results(y, groups, FEATURES, model, comparison, estimation, SEED)
    results = json.loads(json.dumps(results))
    assert results["model_selection"] == "automatic"
    assert results["model_comparison"] == comparison


def test_estimated_classifier_wins_over_external_one():
    with tempfile.TemporaryDirectory() as tmp_dir:
        model_path = Path(tmp_dir) / "model.joblib"
        model_path.touch()
        config = Config(
            {
                "main_directory": tmp_dir,
                "joint_travel": {"features": FEATURES, "external_model": str(model_path)},
                "mode_classifier": {"features": FEATURES, "external_model": str(model_path)},
            }
        )
        external = ExternalJointToursClassifierStep(config)
        assert least_priority_steps({EstimateJointToursClassifierStep(config), external}) == {
            external
        }
        external = ExternalModeClassifierStep(config)
        assert least_priority_steps({EstimateModeClassifierStep(config), external}) == {external}
