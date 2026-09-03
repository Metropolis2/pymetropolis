from loguru import logger

from pymetropolis.common import ThreadedStep
from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.ml_models import estimate_model, get_X, predict, test_models
from pymetropolis.metro_demand.population.files import JointToursFile, ToursFile
from pymetropolis.metro_pipeline import PopulationStep, Step
from pymetropolis.metro_pipeline.parameters import ListParameter, PathParameter, StringParameter
from pymetropolis.metro_pipeline.types import String
from pymetropolis.random import RandomStep

from .files import JointTourEstimatorFile, SurveyedToursFile


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

        tours = self.input["tours"].read()

        # Only fully surveyed households (and on same day) can be used for this estimation
        # (otherwise the "joint" status might be incomplete).
        tours = tours.filter("household_fully_surveyed", "household_surveyed_on_same_day")

        # Households with only one person are automatically not joint.
        tours = tours.filter(pl.col("nb_persons") > 1)

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
