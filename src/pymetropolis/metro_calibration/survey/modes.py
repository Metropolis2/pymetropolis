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
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_pipeline.types import String
from pymetropolis.metro_spatial.simulation_area.file import SimulationAreaFile
from pymetropolis.random import RandomStep

from .files import (
    ModeEstimatorFile,
    SurveyedDrawZonesFile,
    SurveyedToursFile,
    ToursModeShareComparisonFile,
    ToursModeShareDistancePlotFile,
    ToursModeShareTourCountPlotFile,
)

if TYPE_CHECKING:
    import matplotlib.pyplot as plt
    import polars as pl


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


class ModeClassifierConfigStep(Step):
    """Shared survey tour-mode filtering/grouping config for the mode classifier and its
    calibration comparison."""

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

    def filter_survey_tours(self, tours: pl.DataFrame) -> pl.DataFrame:
        """Restricts survey tours to `self.modes` (if set), groups car-driver modes together (if
        `self.group_car_driver_modes`), and drops tours with a null `tour_mode`."""
        import polars as pl

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
                        "car_driver_alone": "car_driver",
                        "car_driver_mixed": "car_driver",
                        "car_driver_with_passengers": "car_driver",
                    }
                )
            )

        tours = tours.filter(pl.col("outside_perimeter").not_())

        return tours.drop_nulls("tour_mode")


class EstimateModeClassifierStep(RandomStep, ThreadedStep, ModeClassifierConfigStep):
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

    input_files = {"tours": SurveyedToursFile}
    output_files = {"estimator": ModeEstimatorFile}

    def is_defined(self):
        return self.features is not None

    def run(self):

        assert self.features is not None

        tours = self.input["tours"].read()

        tours = self.filter_survey_tours(tours)

        X = get_X(tours, self.features)
        # The estimator is fitted on the mode names themselves (not on encoded values) so that
        # `estimator.classes_` can be used to label the predictions of `ClassifyToursModeStep`.
        y = tours["tour_mode"].to_pandas()
        # The tours of a same household are not independent observations (they share all the
        # household-level variables and joint tours are duplicated over the household members), so
        # they must all be assigned to the same cross-validation fold.
        groups = tours["household_id"].to_numpy()
        # What matters for the simulation is not only the share of tours made with each mode, but
        # also the distance travelled with each mode (the vehicle-kilometers). The tours are thus
        # also weighted by their total distance to compute a second fidelity metric.
        weights = tours["total_distance"].to_pandas() if "total_distance" in tours.columns else None
        if self.model is None:
            model = test_models(
                X, y, groups, self.random_seed, self.nb_threads or -1, fidelity_weights=weights
            )
        else:
            model = self.model

        estimator = estimate_model(
            X, y, groups, model, self.random_seed, self.nb_threads or -1, fidelity_weights=weights
        )

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


def restrict_tours_to_area(tours: pl.DataFrame, area) -> pl.DataFrame:
    """Restricts `tours` to those whose origin and destination coordinates (when available) all
    fall within `area` (a shapely geometry in EPSG:4326)."""
    import polars as pl
    import shapely

    coord_cols = ["origin_lngs", "origin_lats", "destination_lngs", "destination_lats"]
    df = tours.select("tour_id", *coord_cols).explode(coord_cols)
    origin_in_area = shapely.contains_xy(
        area, df["origin_lngs"].to_numpy(), df["origin_lats"].to_numpy()
    )
    destination_in_area = shapely.contains_xy(
        area, df["destination_lngs"].to_numpy(), df["destination_lats"].to_numpy()
    )
    df = df.with_columns(
        origin_in_area=pl.Series(origin_in_area), destination_in_area=pl.Series(destination_in_area)
    )
    mask = df.group_by("tour_id").agg(
        keep=pl.col("origin_in_area").all() & pl.col("destination_in_area").all()
    )["keep"]
    return tours.filter(mask)


def plot_mode_share_comparison(
    survey_shares: dict[str, float], sim_shares: dict[str, float], xlabel: str
) -> plt.Figure:
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    modes = sorted(set(survey_shares) | set(sim_shares))
    survey_values = [survey_shares.get(mode, 0.0) for mode in modes]
    sim_values = [sim_shares.get(mode, 0.0) for mode in modes]
    y = list(range(len(modes)))
    height = 0.35

    fig, ax = plt.subplots()
    survey_bars = ax.barh(
        [i + height / 2 for i in y], survey_values, height=height, label="Survey", zorder=1
    )
    sim_bars = ax.barh(
        [i - height / 2 for i in y], sim_values, height=height, label="Simulation", zorder=1
    )
    ax.bar_label(survey_bars, fmt="{:.0%}", padding=5, zorder=3)
    ax.bar_label(sim_bars, fmt="{:.0%}", padding=5, zorder=3)
    ax.set_yticks(y, modes)
    ax.xaxis.set_major_formatter(PercentFormatter(xmax=1, decimals=0))
    ax.set_xlim(left=0)
    ax.set_xlabel(xlabel)
    ax.legend()
    ax.grid(which="major", axis="x", zorder=2)
    fig.tight_layout(pad=0.5)
    return fig


class CompareToursModeSharesStep(PopulationStep, ModeClassifierConfigStep):
    """Compares survey and ex-ante tour mode shares, by tour count and by distance."""

    # TODO. This Step is only valid if the survey area is similar to the simulation area.

    input_files = {
        "survey_tours": SurveyedToursFile,
        "survey_zones": InputFile(SurveyedDrawZonesFile, optional=True),
        "simulation_area": InputFile(SimulationAreaFile, optional=True),
        "sim_tours": ToursFile,
        "sim_modes": ToursModeFile,
    }
    output_files = {
        "comparison": ToursModeShareComparisonFile,
        "tour_count_plot": ToursModeShareTourCountPlotFile,
        "distance_plot": ToursModeShareDistancePlotFile,
    }

    def run(self):
        import json

        import polars as pl

        survey = self.filter_survey_tours(self.input["survey_tours"].read())
        sim = self.input["sim_tours"].read().join(self.input["sim_modes"].read(), on="tour_id")

        if self.input["survey_zones"].exists():
            # Restrict the simulated tours to the surveyed area.
            draw_zones = self.input["survey_zones"].read()
            survey_area = draw_zones.to_crs("EPSG:4326").union_all()
            sim = restrict_tours_to_area(sim, survey_area)

        if self.input["simulation_area"].exists():
            # Restrict the surveyed tours to the simulation area.
            sim_area = self.input["simulation_area"].get_area("EPSG:4326")  # ty: ignore[unresolved-attribute]
            survey = restrict_tours_to_area(survey, sim_area)

        survey_count = (
            survey.group_by("tour_mode")
            .agg(pl.col("weight").sum())
            .with_columns(share=pl.col("weight") / pl.col("weight").sum())
        )
        survey_count_shares = dict(zip(survey_count["tour_mode"], survey_count["share"]))

        sim_count = sim["mode"].value_counts(normalize=True)
        sim_count_shares = dict(zip(sim_count["mode"], sim_count["proportion"]))

        survey_dist = (
            survey.group_by("tour_mode")
            .agg(pl.col("weighted_distance").sum())
            .with_columns(share=pl.col("weighted_distance") / pl.col("weighted_distance").sum())
        )
        survey_dist_shares = dict(zip(survey_dist["tour_mode"], survey_dist["share"]))

        if "total_distance" in sim.columns:
            sim_dist = (
                sim.drop_nulls("total_distance")
                .group_by("mode")
                .agg(pl.col("total_distance").sum())
                .with_columns(share=pl.col("total_distance") / pl.col("total_distance").sum())
            )
            sim_dist_shares = dict(zip(sim_dist["mode"], sim_dist["share"]))
        else:
            logger.warning(
                "`total_distance` is not available in `ToursFile`: skipping the distance-based "
                "mode share comparison for the simulated population."
            )
            sim_dist_shares = {}

        results = {
            "tour_count": {"survey": survey_count_shares, "simulation": sim_count_shares},
            "distance": {"survey": survey_dist_shares, "simulation": sim_dist_shares},
        }
        self.output["comparison"].write(json.dumps(results, indent=2, sort_keys=True))

        fig = plot_mode_share_comparison(survey_count_shares, sim_count_shares, "Share of tours")
        self.output["tour_count_plot"].write(fig)

        fig = plot_mode_share_comparison(survey_dist_shares, sim_dist_shares, "Share of distance")
        self.output["distance_plot"].write(fig)
