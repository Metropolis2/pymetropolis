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
        AdaBoostClassifier,
        ExtraTreesClassifier,
        GradientBoostingClassifier,
        RandomForestClassifier,
    )
    from sklearn.linear_model import LogisticRegression
    from sklearn.neighbors import KNeighborsClassifier
    from sklearn.tree import DecisionTreeClassifier
    # from sklearn.neural_network import MLPClassifier

    return {
        "dummy": DummyClassifier(strategy="stratified", random_state=random_seed),
        "logistic": LogisticRegression(random_state=random_seed),
        "decision_tree": DecisionTreeClassifier(random_state=random_seed),
        "knn": KNeighborsClassifier(),
        "random_forest": RandomForestClassifier(random_state=random_seed),
        "extra_tree": ExtraTreesClassifier(random_state=random_seed),
        "adaboost": AdaBoostClassifier(random_state=random_seed),
        "gradient_boosting": GradientBoostingClassifier(random_state=random_seed),
        # "mlp": MLPClassifier(random_state=random_seed, max_iter=1000),
    }


def get_param_grids():
    return {
        "dummy": {},
        "logistic": {
            "clf__C": [0.01, 0.1, 1, 10, 100],
            "clf__class_weight": ["balanced", None],
            "clf__solver": ["lbfgs", "liblinear"],
        },
        "decision_tree": {
            "clf__max_depth": [None, 5, 10, 20],
            "clf__min_samples_leaf": [1, 5, 10, 20],
            "clf__class_weight": ["balanced", None],
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
            "clf__class_weight": ["balanced", "balanced_subsample"],
        },
        "extra_tree": {
            "clf__n_estimators": [100, 200, 500],
            "clf__max_depth": [None, 5, 10, 20],
            "clf__min_samples_leaf": [1, 5, 10, 20],
            "clf__max_features": ["sqrt", "log2", 0.3],
            "clf__class_weight": ["balanced", "balanced_subsample"],
        },
        "adaboost": {
            "clf__n_estimators": [50, 100, 200, 500],
            "clf__learning_rate": [0.01, 0.1, 0.5, 1.0],
        },
        "gradient_boosting": {
            "clf__n_estimators": [100, 200, 500],
            "clf__max_depth": [2, 3, 5, 10],
            "clf__learning_rate": [0.01, 0.1, 0.5],
            "clf__min_samples_leaf": [1, 5, 10, 20],
            "clf__subsample": [0.5, 0.8, 1.0],
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
    return tours.select(*features).to_pandas()


def get_preprocessor():
    import numpy as np
    from sklearn.compose import ColumnTransformer, make_column_selector
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

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
            ("num", numeric_transformer, make_column_selector(dtype_include=np.number)),
            (
                "cat",
                categorical_transformer,
                make_column_selector(dtype_include=[object, "string"]),
            ),
        ]
    )
    return preprocessor


def test_models(X: pd.DataFrame, y: pd.Series, random_seed: int | None, nb_threads: int | None):
    from sklearn.model_selection import StratifiedKFold, cross_validate
    from sklearn.pipeline import Pipeline

    classifiers = get_classifiers(random_seed)
    preprocessor = get_preprocessor()
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=random_seed)

    results = {}
    for model, classifier in classifiers.items():
        logger.debug(f"Evaluating {model}...")
        pipe = Pipeline([("pre", preprocessor), ("clf", classifier)])
        cv_res = cross_validate(pipe, X, y, cv=cv, scoring="neg_brier_score", n_jobs=nb_threads)
        results[model] = cv_res["test_score"].mean()

    best_model = sorted(results.items(), key=lambda i: i[1], reverse=True)[0]
    logger.info(
        f"Best model: {best_model[0]}; Brier score: {-best_model[1]:.2%}; "
        f"Stratified dummy score: {-results['dummy']:.2%}"
    )
    return best_model[0]


def _grid_size(param_grid: dict) -> int:
    size = 1
    for values in param_grid.values():
        size *= len(values)
    return size


def estimate_model(
    X: pd.DataFrame, y: pd.Series, model: str, random_seed: int | None, nb_threads: int | None
) -> BaseEstimator:
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.model_selection import RandomizedSearchCV, StratifiedKFold
    from sklearn.pipeline import Pipeline

    classifiers = get_classifiers(random_seed)
    preprocessor = get_preprocessor()
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=random_seed)

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
        scoring="neg_brier_score",
        cv=cv,
        n_jobs=nb_threads,
        random_state=random_seed,
    )
    search.fit(X, y)

    logger.debug(f"Best parameters: {search.best_params_}")
    logger.debug(f"Best CV Brier score: {-search.best_score_:.2%}")

    # Estimate a calibrated model.
    calibrated = CalibratedClassifierCV(
        search.best_estimator_, method="sigmoid", cv=cv, n_jobs=nb_threads
    )
    calibrated.fit(X, y)
    return calibrated


def predict(X: pd.DataFrame, estimator: BaseEstimator, rng: np.random.Generator):
    probs = estimator.predict_proba(X)[:, 1]  # ty: ignore[unresolved-attribute]
    return rng.binomial(n=1, p=probs).astype(bool)


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
