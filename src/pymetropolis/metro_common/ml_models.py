from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_common import MetropyError

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd
    import polars as pl
    from sklearn.base import BaseEstimator


def get_classifiers(random_seed: int | None):
    from sklearn.dummy import DummyClassifier
    from sklearn.ensemble import (
        ExtraTreesClassifier,
        HistGradientBoostingClassifier,
        RandomForestClassifier,
    )
    from sklearn.linear_model import LogisticRegression
    from sklearn.neighbors import KNeighborsClassifier
    from sklearn.tree import DecisionTreeClassifier
    # from sklearn.neural_network import MLPClassifier

    # Note. The hyperparameters set below are the ones used by `test_models` to compare the
    # classifiers with each other (`estimate_model` searches over `get_param_grids` instead). They
    # depart from the scikit-learn defaults where those are tuned for hard classification: the
    # classifiers here are compared with a Brier score and their probabilities are meant to be
    # sampled from, which requires leaves / neighborhoods large enough to estimate a probability
    # (with the scikit-learn defaults, a tree leaf holds a single observation, so the predicted
    # probabilities are all 0 or 1).
    return {
        # The class priors are the reference "no-information" probabilities. With the
        # `"stratified"` strategy, the dummy classifier draws a class at random instead, i.e. it
        # predicts probabilities of 0 or 1, which is a much weaker baseline.
        "dummy": DummyClassifier(strategy="prior"),
        "logistic": LogisticRegression(random_state=random_seed, max_iter=1000),
        "decision_tree": DecisionTreeClassifier(random_state=random_seed, min_samples_leaf=20),
        "knn": KNeighborsClassifier(n_neighbors=50),
        "random_forest": RandomForestClassifier(random_state=random_seed, min_samples_leaf=5),
        "extra_tree": ExtraTreesClassifier(random_state=random_seed, min_samples_leaf=5),
        # The histogram-based implementation is used rather than `GradientBoostingClassifier` for
        # best speed on large datasets.
        "gradient_boosting": HistGradientBoostingClassifier(
            random_state=random_seed,
            early_stopping=False,
            learning_rate=0.1,
            max_leaf_nodes=31,
            min_samples_leaf=100,
            l2_regularization=10.0,
            max_iter=200,
        ),
        # Classifiers disabled (poor performances).
        # "adaboost": AdaBoostClassifier(random_state=random_seed),
        # "mlp": MLPClassifier(random_state=random_seed, max_iter=1000),
    }


def get_param_grids():
    # Note. `class_weight` is not part of the grids: the predicted probabilities are meant to be
    # sampled from (so that the predicted shares match the observed ones) and re-weighting the
    # classes distorts them (e.g., with `class_weight="balanced"`, the probabilities are those of
    # a population where all the classes are equally frequent). The classifiers are thus always
    # fitted with their default `class_weight=None`.
    return {
        "dummy": {},
        "logistic": {"clf__C": [0.001, 0.01, 0.1, 1, 10, 100, 1000]},
        "decision_tree": {
            "clf__max_depth": [None, 5, 10, 20],
            "clf__min_samples_leaf": [1, 5, 10, 20],
        },
        "knn": {
            "clf__n_neighbors": [3, 5, 10, 20, 50],
            "clf__weights": ["uniform", "distance"],
            "clf__p": [1, 2],
        },
        "random_forest": {
            "clf__n_estimators": [100, 200, 500],
            "clf__max_depth": [None, 5, 10, 20],
            "clf__min_samples_leaf": [1, 5, 10, 20],
            "clf__max_features": ["sqrt", "log2", 0.3],
        },
        "extra_tree": {
            "clf__n_estimators": [100, 200, 500],
            "clf__max_depth": [None, 5, 10, 20],
            "clf__min_samples_leaf": [1, 5, 10, 20],
            "clf__max_features": ["sqrt", "log2", 0.3],
        },
        "adaboost": {
            "clf__n_estimators": [50, 100, 200, 500],
            "clf__learning_rate": [0.01, 0.1, 0.5, 1.0],
        },
        # Note. The hyperparameters are those of `HistGradientBoostingClassifier`: the number of
        # iterations is `max_iter` (not `n_estimators`), the complexity of each tree is bounded by
        # `max_leaf_nodes` (not `max_depth`) and there is no `subsample`.
        # "gradient_boosting": {
        #     "clf__max_iter": [100, 200, 500],
        #     "clf__learning_rate": [0.01, 0.05, 0.1],
        #     "clf__max_leaf_nodes": [8, 15, 31],
        #     "clf__min_samples_leaf": [20, 50, 100],
        #     "clf__l2_regularization": [0.0, 1.0, 10.0],
        # },
        "gradient_boosting": {
            "clf__max_iter": [200],
            "clf__learning_rate": [0.1],
            "clf__max_leaf_nodes": [31],
            "clf__min_samples_leaf": [100],
            "clf__l2_regularization": [10.0],
        },
    }


def get_X(tours: pl.DataFrame, features: list[str]) -> pd.DataFrame:
    import polars as pl
    import polars.selectors as cs

    has_error = False
    for feat in features:
        if feat not in tours.columns:
            logger.error(f"Missing column `{feat}` in tours.")
            has_error = True
    if has_error:
        raise MetropyError("Cannot generate tours with all features")

    tours = tours.with_columns(cs.boolean().cast(pl.Int64), cs.duration().dt.total_seconds())
    X = tours.select(*features).to_pandas()

    # The ColumnTransformer of `get_preprocessor` drops the columns that no selector matches, so
    # any feature with an unhandled dtype would be silently excluded from the model.
    numeric_selector, categorical_selector = get_column_selectors()
    handled = set(numeric_selector(X)) | set(categorical_selector(X))
    if ignored := [col for col in X.columns if col not in handled]:
        logger.warning(
            "The following features have a dtype that the preprocessor does not handle, they are "
            f"ignored by the model: {', '.join(ignored)}"
        )

    return X


def get_column_selectors():
    """Returns the selectors of the numeric and categorical columns used by the preprocessor."""
    import numpy as np
    from sklearn.compose import make_column_selector

    numeric_selector = make_column_selector(dtype_include=np.number)
    # Polars Enum columns become pandas categorical columns when converted to pandas, they must be
    # selected as categorical variables (otherwise they are dropped by the ColumnTransformer).
    categorical_selector = make_column_selector(dtype_include=[object, "string", "category"])
    return numeric_selector, categorical_selector


def get_preprocessor():
    from sklearn.compose import ColumnTransformer
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    numeric_selector, categorical_selector = get_column_selectors()
    numeric_transformer = Pipeline(
        [("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())]
    )
    categorical_transformer = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]
    )
    preprocessor = ColumnTransformer(
        [
            ("num", numeric_transformer, numeric_selector),
            ("cat", categorical_transformer, categorical_selector),
        ]
    )
    return preprocessor


def get_cv_splits(
    X: pd.DataFrame, y: pd.Series, groups: np.ndarray, random_seed: int | None, n_splits: int = 5
) -> list[tuple[np.ndarray, np.ndarray]]:
    """Returns the (train, test) indices of the cross-validation folds.

    The folds are stratified by `y` and grouped by `groups`: all the observations sharing a group
    are assigned to the same fold. Grouping is required whenever the observations are not
    independent (e.g., the tours of the members of a same household share all their household-level
    variables and joint tours are duplicated over the household members): without it, near-copies
    of a test observation are part of the training set and the scores are over-optimistic.

    The folds are returned as a list of indices (instead of a splitter) so that the exact same
    folds can be re-used by the estimators that do not accept `groups` in `fit`.
    """
    from sklearn.model_selection import StratifiedGroupKFold

    cv = StratifiedGroupKFold(n_splits=n_splits, shuffle=True, random_state=random_seed)
    return list(cv.split(X, y, groups))


def fidelity(
    y_true: np.ndarray, y_proba: np.ndarray, labels: np.ndarray, weights: pd.Series | None = None
) -> float:
    """Returns the gap between the predicted and the observed share of each class.

    The gap is the sum, over the classes, of the absolute difference between the predicted share of
    a class and its observed share (so it is zero for a perfect fit and lower is better). Contrary
    to the Brier score, which measures how good the predictions are for each individual
    observation, this measures how good they are in aggregate: it is the expected error on the
    shares of a population whose classes are drawn from the predicted probabilities (as
    `sample_classes` does).

    With `weights`, the shares are weighted by observation instead of being simple counts. Weighting
    the tours by their total distance, for example, gives the expected error on the distance
    travelled with each mode (rather than on the number of tours made with each mode), which is
    what matters for the vehicle-kilometers simulated by METROPOLIS2. `weights` is indexed like the
    `y` the scorer was built from, so that the weights of a cross-validation fold can be recovered
    from the index of `y_true`.
    """
    import numpy as np

    labels = np.asarray(labels)
    if y_proba.shape[1] != len(labels):
        # A class is missing from the training fold: the predicted probabilities cannot be matched
        # with the classes.
        return np.nan
    if weights is None:
        w = np.ones(len(y_proba))
    else:
        w = weights.loc[y_true.index].to_numpy()  # ty: ignore[unresolved-attribute]
        # A null weight is a null contribution to the shares.
        w = np.nan_to_num(w, nan=0.0)
    total = w.sum()
    if total <= 0:
        return np.nan
    is_class = np.asarray(y_true)[:, np.newaxis] == labels[np.newaxis, :]
    observed_shares = (w[:, np.newaxis] * is_class).sum(axis=0) / total
    predicted_shares = (w[:, np.newaxis] * y_proba).sum(axis=0) / total
    return float(np.abs(predicted_shares - observed_shares).sum())


def get_fidelity_scorer(y: pd.Series, weights: pd.Series | None = None):
    """Returns a scikit-learn scorer for the `fidelity` metric (negated, as scorers are maximized).

    The classes are read from `y` (and not from each fold) because a scorer only sees the
    probabilities, whose columns are ordered like the sorted classes of the fitted estimator.
    """
    import numpy as np
    from sklearn.metrics import make_scorer

    return make_scorer(
        fidelity,
        response_method="predict_proba",
        greater_is_better=False,
        labels=np.unique(y),
        weights=weights,
    )


def get_scoring(y: pd.Series, fidelity_weights: pd.Series | None = None) -> dict:
    """Returns the metrics computed for each model / candidate.

    `weighted_fidelity` is only included when `fidelity_weights` is given.
    """
    scoring = {"brier": "neg_brier_score", "fidelity": get_fidelity_scorer(y)}
    if fidelity_weights is not None:
        scoring["weighted_fidelity"] = get_fidelity_scorer(y, fidelity_weights)
    return scoring


def test_models(
    X: pd.DataFrame,
    y: pd.Series,
    groups: np.ndarray,
    random_seed: int | None,
    nb_threads: int | None,
    fidelity_weights: pd.Series | None = None,
):
    from sklearn.model_selection import cross_validate
    from sklearn.pipeline import Pipeline

    classifiers = get_classifiers(random_seed)
    preprocessor = get_preprocessor()
    cv = get_cv_splits(X, y, groups, random_seed)

    scoring = get_scoring(y, fidelity_weights)

    briers = {}
    fidelities = {}
    weighted_fidelities = {}
    for model, classifier in classifiers.items():
        logger.debug(f"Evaluating {model}...")
        pipe = Pipeline([("pre", preprocessor), ("clf", classifier)])
        cv_res = cross_validate(pipe, X, y, cv=cv, scoring=scoring, n_jobs=nb_threads)
        briers[model] = cv_res["test_brier"].mean()
        fidelities[model] = -cv_res["test_fidelity"].mean()
        if "test_weighted_fidelity" in cv_res:
            weighted_fidelities[model] = -cv_res["test_weighted_fidelity"].mean()

    ranking = sorted(briers.items(), key=lambda i: i[1], reverse=True)
    logger.info("Model comparison (Brier score and fidelities, lower is better for all):")
    for model, brier in ranking:
        msg = f"  {model}: Brier score: {-brier:.2%}; fidelity: {fidelities[model]:.2%}"
        if model in weighted_fidelities:
            msg += f"; weighted fidelity: {weighted_fidelities[model]:.2%}"
        logger.info(msg)

    best_model = ranking[0]
    logger.info(
        f"Best model: {best_model[0]}; Brier score: {-best_model[1]:.2%}; "
        f"Class-priors dummy score: {-briers['dummy']:.2%}"
    )
    return best_model[0]


def _grid_size(param_grid: dict) -> int:
    size = 1
    for values in param_grid.values():
        size *= len(values)
    return size


def estimate_model(
    X: pd.DataFrame,
    y: pd.Series,
    groups: np.ndarray,
    model: str,
    random_seed: int | None,
    nb_threads: int | None,
    fidelity_weights: pd.Series | None = None,
) -> BaseEstimator:
    from sklearn.model_selection import RandomizedSearchCV
    from sklearn.pipeline import Pipeline

    classifiers = get_classifiers(random_seed)
    preprocessor = get_preprocessor()
    cv = get_cv_splits(X, y, groups, random_seed)

    classifier = classifiers.get(model)
    if classifier is None:
        raise MetropyError(f"Unsupported classification model: {model}")

    param_grid = get_param_grids().get(model)
    if param_grid is None:
        raise MetropyError(f"No hyperparameter grid defined for model: {model}")

    pipe = Pipeline([("pre", preprocessor), ("clf", classifier)])
    search = RandomizedSearchCV(
        pipe,
        param_distributions=param_grid,
        n_iter=min(50, max(1, _grid_size(param_grid))),
        # The fidelity is only reported, the hyperparameters are selected on the Brier score alone
        # (`refit`). Computing it is free: it is another metric over the same predictions, not
        # another fit.
        scoring=get_scoring(y, fidelity_weights),
        refit="brier",
        cv=cv,
        n_jobs=nb_threads,
        random_state=random_seed,
        verbose=2,
    )
    search.fit(X, y)

    logger.debug(f"Best parameters: {search.best_params_}")
    msg = (
        f"Best CV Brier score: {-search.best_score_:.2%}; "
        f"fidelity: {-search.cv_results_['mean_test_fidelity'][search.best_index_]:.2%}"
    )
    if "mean_test_weighted_fidelity" in search.cv_results_:
        weighted = -search.cv_results_["mean_test_weighted_fidelity"][search.best_index_]
        msg += f"; weighted fidelity: {weighted:.2%}"
    logger.debug(msg)

    # Note. The estimator is not re-calibrated (e.g., with a `CalibratedClassifierCV`): the
    # probabilities are meant to be sampled from and the one-vs-rest calibration of a multiclass
    # classifier does not preserve the predicted shares (it makes them less accurate in practice).
    return search.best_estimator_


def predict(X: pd.DataFrame, estimator: BaseEstimator, rng: np.random.Generator):
    """Draws a boolean outcome for each observation, from a binary classifier."""
    probs = estimator.predict_proba(X)  # ty: ignore[unresolved-attribute]
    if probs.shape[1] != 2:
        raise MetropyError(
            f"`predict` requires a binary classifier but the estimator has {probs.shape[1]} "
            "classes; use `sample_classes` instead"
        )
    return rng.binomial(n=1, p=probs[:, 1]).astype(bool)


def sample_classes(
    X: pd.DataFrame, estimator: BaseEstimator, rng: np.random.Generator
) -> np.ndarray:
    """Draws a class for each observation, from the probabilities of a classifier.

    The classes drawn are the labels the estimator was fitted on (`estimator.classes_`), so the
    estimator must have been fitted with the actual labels, not with encoded values.
    """
    import numpy as np

    probs = estimator.predict_proba(X)  # ty: ignore[unresolved-attribute]
    classes = np.asarray(estimator.classes_)  # ty: ignore[unresolved-attribute]
    # Inverse-transform sampling: draw one uniform per observation and find the first class whose
    # cumulated probability exceeds it.
    cumulated_probs = probs.cumsum(axis=1)
    # Guard against floating-point round-off leaving the last cumulated probability below 1.
    cumulated_probs[:, -1] = 1.0
    draws = rng.random(len(probs))
    indices = (draws[:, np.newaxis] < cumulated_probs).argmax(axis=1)
    return classes[indices]


def compute_lasso(
    endog_variable: pl.Series, exog_variables: pl.DataFrame
) -> tuple[np.ndarray, np.ndarray, float, dict[str, float]]:
    import numpy as np
    from sklearn.linear_model import LassoCV
    from sklearn.metrics import root_mean_squared_error

    logger.info("Fitting a LASSO model...")
    Y = endog_variable.to_numpy()
    X = exog_variables.to_numpy()
    if X.ndim != 2:
        raise MetropyError(f"Endog variables should be 2-dimensional:\n{X}")
    if Y.ndim != 1:
        raise MetropyError(f"Exog variables should be 1-dimensional:\n{Y}")
    if X.shape[0] != Y.shape[0]:
        raise MetropyError(
            f"Mismatched number of observations: endog: {Y.shape[0]} / exog: {X.shape[0]}"
        )
    logger.debug(f"Number of observations: {X.shape[0]}")
    logger.debug(f"Number of variables: {X.shape[1]}")
    lassocv = LassoCV(fit_intercept=False, max_iter=10_000)
    lassocv.fit(X, Y)
    logger.debug(f"Value of the penalization factor: {lassocv.alpha_}")
    Y_hat = lassocv.predict(X)
    residuals = Y - Y_hat
    rmse = root_mean_squared_error(Y, Y_hat)
    logger.debug(f"RMSE: {rmse}")
    corr = np.corrcoef(Y, Y_hat)[0][1]
    logger.debug(f"Correlation: {corr:.8%}")
    coefs = lassocv.coef_
    coef_lasso = {var: coef for var, coef in zip(exog_variables.columns, coefs)}
    return (Y_hat, residuals, rmse, coef_lasso)
