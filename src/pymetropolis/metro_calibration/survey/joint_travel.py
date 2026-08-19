from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.common import ThreadedStep
from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_demand.population.files import JointToursFile, ToursFile
from pymetropolis.metro_pipeline.parameters import ListParameter, StringParameter
from pymetropolis.metro_pipeline.types import String
from pymetropolis.random import RandomStep

from .files import JointTourEstimatorFile, SurveyedToursFile

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


def classify_joint_tours(X: pd.DataFrame, estimator: BaseEstimator, rng: np.random.Generator):
    probs = estimator.predict_proba(X)[:, 1]  # ty: ignore[unresolved-attribute]
    return rng.binomial(n=1, p=probs).astype(bool)


class EstimateJointToursClassifierStep(RandomStep, ThreadedStep):
    """Estimates a Machine Learning model to classify joint tours."""

    features = ListParameter("joint_travel.features", inner=String(), description="TODO")
    model = StringParameter("joint_travel.model", description="TODO")

    input_files = {"tours": SurveyedToursFile}
    output_files = {"estimator": JointTourEstimatorFile}

    def is_defined(self):
        return self.features is not None

    def run(self):
        import polars as pl

        assert self.features is not None

        tours = self.input["tours"].read()

        # Only fully surveyed households (and on same day) can be used for this estimation
        # (otherwise the "joint" status might be incomplete).
        tours = tours.filter("household_fully_surveyed", "household_surveyed_on_same_day")

        # Households with only one person are automatically not joint.
        tours = tours.filter(pl.col("nb_persons") > 1)

        X = get_X(tours, self.features)
        y = tours["joint_tour"].cast(pl.Int64).to_pandas()
        if self.model is None:
            model = test_models(X, y, self.random_seed, self.nb_threads or -1)
        else:
            model = self.model

        estimator = estimate_model(X, y, model, self.random_seed, self.nb_threads or -1)

        self.output["estimator"].write(estimator)


class ClassifyJointToursStep(RandomStep):
    input_files = {"tours": ToursFile, "estimator": JointTourEstimatorFile}
    output_files = {"joint_tours": JointToursFile}

    def run(self):
        import polars as pl

        tours = self.input["tours"].read()
        estimator = self.input["estimator"].read()

        features = estimator.feature_names_in_

        X = get_X(tours.filter(pl.col("nb_persons") > 1), features)

        joint_tour_flag = classify_joint_tours(X, estimator, self.get_rng())

        # Force `joint_tour` = false for all tours with `nb_persons` = 1.
        df = pl.concat(
            (
                tours.filter(nb_persons=1).select("tour_id", joint_tour=False),
                tours.filter(pl.col("nb_persons") > 1).select(
                    "tour_id", joint_tour=pl.Series(joint_tour_flag)
                ),
            ),
            how="vertical",
        )

        s = df["joint_tour"].mean()
        logger.debug(f"Share of joint tours: {s:.2%}")

        self.output["joint_tours"].write(df)
