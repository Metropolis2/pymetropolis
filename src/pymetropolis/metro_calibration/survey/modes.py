from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.common import ThreadedStep
from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.ml_models import estimate_model, get_X, sample_classes, test_models
from pymetropolis.metro_demand.population.files import ToursFile, ToursModeFile
from pymetropolis.metro_pipeline import PopulationStep, Step
from pymetropolis.metro_pipeline.parameters import (
    BoolParameter,
    ListParameter,
    PathParameter,
    StringParameter,
)
from pymetropolis.metro_pipeline.types import String
from pymetropolis.random import RandomStep

from .files import ModeEstimatorFile, SurveyedToursFile

if TYPE_CHECKING:
    pass


class ExternalModeClassifierStep(Step):
    """Import an estimator for modes classification from another Pymetropolis run."""

    path = PathParameter(
        "mode_classifier.external_model",
        check_file_exists=True,
        description="Path to a .joblib file with the classifier for modes.",
    )

    output_files = {"estimator": ModeEstimatorFile}

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


class EstimateModeClassifierStep(RandomStep, ThreadedStep):
    """Estimates a Machine Learning model to predict tour-level modes."""

    features = ListParameter(
        "mode_classifier.features",
        inner=String(),
        description="List of variables to use in the model.",
        note="Variables should be available in `SurveyedToursFile`.",
    )
    model = StringParameter(
        "mode_classifier.model",
        description="ML model to use as classifier.",
        note="If no model is specified, multiple classifiers are run and the best one is selected.",
    )
    modes = ListParameter(
        "mode_classifier.modes",
        inner=String(),
        description="Valid modes to be used in the model.",
        note="If not specified, all modes are used.",
    )
    group_car_driver_modes = BoolParameter(
        "mode_classifier.group_car_driver_modes",
        default=False,
        description="If `true`, all car-driver modes are grouped in a single category.",
    )

    input_files = {"tours": SurveyedToursFile}
    output_files = {"estimator": ModeEstimatorFile}

    def is_defined(self):
        return self.features is not None

    def run(self):
        import polars as pl

        assert self.features is not None

        tours = self.input["tours"].read()

        if self.modes is not None:
            n0 = len(tours)
            tours = tours.filter(pl.col("tour_mode").is_in(self.modes))
            n1 = len(tours)
            if n1 < n0:
                s = (n0 - n1) / n0
                logger.warning(f"Dropping {n0 - n1:,} observations ({s:.2%}) with invalid modes.")

        if self.group_car_driver_modes:
            tours = tours.with_columns(
                tour_mode=pl.col("tour_mode").replace(
                    {
                        "car_driver_mixed": "car_driver_alone",
                        "car_driver_with_passengers": "car_driver_alone",
                    }
                )
            )

        tours = tours.drop_nulls("tour_mode")

        X = get_X(tours, self.features)
        # The estimator is fitted on the mode names themselves (not on encoded values) so that
        # `estimator.classes_` can be used to label the predictions of `ClassifyToursModeStep`.
        y = tours["tour_mode"].to_pandas()
        # The tours of a same household are not independent observations (they share all the
        # household-level variables and joint tours are duplicated over the household members), so
        # they must all be assigned to the same cross-validation fold.
        groups = tours["household_id"].to_numpy()
        if self.model is None:
            model = test_models(X, y, groups, self.random_seed, self.nb_threads or -1)
        else:
            model = self.model

        estimator = estimate_model(X, y, groups, model, self.random_seed, self.nb_threads or -1)

        self.output["estimator"].write(estimator)


class ClassifyToursModeStep(RandomStep, PopulationStep):
    """Predict an ex-ante mode for each tour, based on the mode classifier."""

    input_files = {"tours": ToursFile, "estimator": ModeEstimatorFile}
    output_files = {"modes": ToursModeFile}

    def run(self):
        import polars as pl

        tours = self.input["tours"].read()
        estimator = self.input["estimator"].read()

        features = estimator.feature_names_in_

        X = get_X(tours, features)

        modes = sample_classes(X, estimator, self.get_rng(str(self)))

        df = tours.select("tour_id", mode=pl.Series("mode", modes, dtype=pl.String))

        for mode, count in df["mode"].value_counts(sort=True).iter_rows():
            logger.info(f"Share of tours with mode `{mode}`: {count / len(df):.2%}")

        self.output["modes"].write(df)
