import numpy as np
import pandas as pd
import pytest
from sklearn.pipeline import Pipeline
from sklearn.tree import DecisionTreeClassifier

from pymetropolis.metro_common.ml_models import compute_feature_importance, get_preprocessor
from pymetropolis.metro_common.plots import plot_feature_importance

SEED = 1907

FEATURES = ["signal", "weak_signal", "noise"]


def get_data():
    """Returns a dataset where `signal` fully determines the target, `weak_signal` is correlated
    with it and `noise` is independent from it.

    The observations are grouped by pairs (as the tours of a household are), so that the
    cross-validation splits have something to group on.
    """
    rng = np.random.default_rng(SEED)
    n = 400
    signal = rng.normal(size=n)
    y = pd.Series((signal > 0).astype(int))
    # A categorical variable: the correct class 80% of the time.
    flipped = rng.random(n) < 0.2
    weak_signal = np.where(y.to_numpy() ^ flipped, "high", "low")
    X = pd.DataFrame(
        {
            "signal": signal,
            "weak_signal": pd.Series(weak_signal, dtype="category"),
            "noise": rng.normal(size=n),
        }
    )
    groups = np.repeat(np.arange(n // 2), 2)
    return X, y, groups


def get_estimator():
    return Pipeline(
        [
            ("pre", get_preprocessor()),
            ("clf", DecisionTreeClassifier(random_state=SEED, min_samples_leaf=20)),
        ]
    )


def get_importance():
    X, y, groups = get_data()
    return compute_feature_importance(
        X, y, groups, get_estimator(), SEED, nb_threads=1, n_repeats=3
    )


def test_one_row_per_feature():
    df = get_importance()
    assert df.columns == ["feature", "importance", "importance_std"]
    assert sorted(df["feature"].to_list()) == sorted(FEATURES)
    assert df["importance"].null_count() == 0
    assert df["importance_std"].null_count() == 0


def test_sorted_by_decreasing_importance():
    df = get_importance()
    importances = df["importance"].to_list()
    assert importances == sorted(importances, reverse=True)


def test_informative_features_rank_first():
    """The feature that determines the target is the most important one and the feature
    independent from it has a null importance."""
    df = get_importance()
    assert df["feature"].to_list() == ["signal", "weak_signal", "noise"]
    importances = dict(zip(df["feature"], df["importance"]))
    assert importances["signal"] > 0.1
    assert importances["noise"] == pytest.approx(0.0, abs=0.01)


def test_plot_shows_all_the_features_by_default():
    df = get_importance()
    fig = plot_feature_importance(df, "importance")
    labels = [t.get_text() for t in fig.axes[0].get_yticklabels()]
    # The bars are drawn from the bottom up, so the labels are in increasing order of importance.
    assert labels == ["noise", "weak_signal", "signal"]


def test_plot_is_limited_to_the_most_important_features():
    df = get_importance()
    fig = plot_feature_importance(df, "importance", max_features=2)
    labels = [t.get_text() for t in fig.axes[0].get_yticklabels()]
    assert labels == ["weak_signal", "signal"]
