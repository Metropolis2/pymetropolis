from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.common import ThreadedStep
from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.ml_models import (
    compute_feature_importance,
    estimate_model,
    get_X,
    predict,
    test_models,
)
from pymetropolis.metro_common.plots import plot_feature_importance
from pymetropolis.metro_demand.population.files import JointToursFile, ToursFile
from pymetropolis.metro_pipeline import PopulationStep, Step
from pymetropolis.metro_pipeline.parameters import (
    BoolParameter,
    IntParameter,
    ListParameter,
    PathParameter,
    StringParameter,
)
from pymetropolis.metro_pipeline.types import String
from pymetropolis.random import RandomStep

from .files import (
    JointTourEstimatorFile,
    JointTourFeatureImportanceFile,
    JointTourFeatureImportancePlotFile,
    SurveyedToursFile,
)

if TYPE_CHECKING:
    import polars as pl


class ExternalJointToursClassifierStep(Step):
    """Import an estimator for joint-tours classification from another Pymetropolis run."""

    path = PathParameter(
        "joint_travel.external_model",
        check_file_exists=True,
        description="Path to a .joblib file with the classifier for joint tours.",
    )

    output_files = {"estimator": JointTourEstimatorFile}

    def is_defined(self):
        return self.path is not None

    def run(self):
        import joblib

        assert self.path is not None

        try:
            with open(self.path, "rb") as f:
                estimator = joblib.load(f)
        except Exception as e:
            raise MetropyError(f"Failed to read joblib estimator from `{self.path}`:\n{e}")

        self.output["estimator"].write(estimator)


def filter_joint_tour_observations(tours: pl.DataFrame) -> pl.DataFrame:
    """Restricts survey tours to the observations the joint-tour classifier is estimated on."""
    import polars as pl

    # Only fully surveyed households (and on same day) can be used for this estimation
    # (otherwise the "joint" status might be incomplete).
    tours = tours.filter("household_fully_surveyed", "household_surveyed_on_same_day")

    # Households with only one person are automatically not joint.
    return tours.filter(pl.col("nb_persons") > 1)


class EstimateJointToursClassifierStep(RandomStep, ThreadedStep):
    """Estimates a Machine Learning model to classify joint tours."""

    features = ListParameter(
        "joint_travel.features",
        inner=String(),
        description="List of variables to use in the model.",
        note="Variables should be available in `SurveyedToursFile`.",
    )
    model = StringParameter(
        "joint_travel.model",
        description="ML model to use as classifier.",
        note="If no model is specified, multiple classifiers are run and the best one is selected.",
    )

    input_files = {"tours": SurveyedToursFile}
    output_files = {"estimator": JointTourEstimatorFile}

    def is_defined(self):
        return self.features is not None

    def run(self):
        import polars as pl

        assert self.features is not None

        tours = filter_joint_tour_observations(self.input["tours"].read())

        X = get_X(tours, self.features)
        y = tours["joint_tour"].cast(pl.Int64).to_pandas()
        # A joint tour is defined by the co-travel of household members, so the tours of a same
        # household must all be assigned to the same cross-validation fold.
        groups = tours["household_id"].to_numpy()
        if self.model is None:
            model = test_models(X, y, groups, self.random_seed, self.nb_threads or -1)
        else:
            model = self.model

        estimator = estimate_model(X, y, groups, model, self.random_seed, self.nb_threads or -1)

        self.output["estimator"].write(estimator)


class JointToursFeatureImportanceStep(RandomStep, ThreadedStep):
    """Computes the permutation feature importance of the joint-tour classifier."""

    feature_importance = BoolParameter(
        "joint_travel.feature_importance",
        default=False,
        description=(
            "Whether to compute a feature importance analysis of the joint-tour classifier."
        ),
    )

    nb_plotted_features = IntParameter(
        "joint_travel.nb_plotted_features",
        lower_bound=1,
        description="Number of features to show in the feature importance plot.",
        note=(
            "Only the most important features are shown. "
            "Default is to show all the features. "
            "This does not affect `JointTourFeatureImportanceFile`, which always includes all the "
            "features."
        ),
    )

    input_files = {"tours": SurveyedToursFile, "estimator": JointTourEstimatorFile}
    output_files = {
        "importance": JointTourFeatureImportanceFile,
        "plot": JointTourFeatureImportancePlotFile,
    }

    def is_defined(self):
        return bool(self.feature_importance)

    def run(self):
        import polars as pl

        tours = filter_joint_tour_observations(self.input["tours"].read())
        estimator = self.input["estimator"].read()

        # The features are read from the estimator (and not from `joint_travel.features`) so that
        # the analysis always describes the estimator actually stored on disk, including one
        # imported by `ExternalJointToursClassifierStep`.
        features = estimator.feature_names_in_

        X = get_X(tours, features)
        y = tours["joint_tour"].cast(pl.Int64).to_pandas()
        # Same grouping as for the estimation: the tours of a same household must all be assigned
        # to the same cross-validation fold.
        groups = tours["household_id"].to_numpy()

        df = compute_feature_importance(
            X, y, groups, estimator, self.random_seed, self.nb_threads or -1
        )

        logger.info("Feature importance of the joint-tour classifier (decrease in Brier score):")
        for feature, importance, std in df.head(10).iter_rows():
            logger.info(f"  {feature}: {importance:.4f} (+/- {std:.4f})")

        self.output["importance"].write(df)
        fig = plot_feature_importance(
            df,
            "Decrease in Brier score when the feature is shuffled",
            max_features=self.nb_plotted_features,
        )
        self.output["plot"].write(fig)


class ClassifyJointToursStep(RandomStep, PopulationStep):
    """Classifies tours as joint or non-joint, based on the joint-tour classifier."""

    input_files = {"tours": ToursFile, "estimator": JointTourEstimatorFile}
    output_files = {"joint_tours": JointToursFile}

    def run(self):
        import polars as pl

        tours = self.input["tours"].read()
        estimator = self.input["estimator"].read()

        features = estimator.feature_names_in_

        X = get_X(tours.filter(pl.col("nb_persons") > 1), features)

        joint_tour_flag = predict(X, estimator, self.get_rng(str(self)))

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
