from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.common import ThreadedStep
from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.ml_models import (
    compute_feature_importance,
    estimate_model,
    estimation_results,
    get_X,
    predict,
    test_models,
)
from pymetropolis.metro_common.plots import (
    PlotStep,
    plot_feature_importance,
    plot_joint_share_comparison,
)
from pymetropolis.metro_demand.population.common import TOUR_VARIABLE_LABELS
from pymetropolis.metro_demand.population.files import (
    HouseholdsHomesFile,
    JointToursFile,
    ToursFile,
    TripsFile,
)
from pymetropolis.metro_pipeline import PopulationStep, Step
from pymetropolis.metro_pipeline.parameters import (
    BoolParameter,
    IntParameter,
    ListParameter,
    PathParameter,
    StringParameter,
)
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_pipeline.types import String
from pymetropolis.metro_spatial.simulation_area.file import SimulationAreaFile
from pymetropolis.random import RandomStep

from .files import (
    JointTourEstimationResultsFile,
    JointTourEstimatorFile,
    JointTourFeatureImportanceFile,
    JointTourFeatureImportancePlotFile,
    JointToursShareComparisonFile,
    JointToursShareDistancePlotFile,
    JointToursShareTourCountPlotFile,
    SurveyedDrawZonesFile,
    SurveyedHouseholdsFile,
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
    output_files = {"estimator": JointTourEstimatorFile, "results": JointTourEstimationResultsFile}

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
            model, comparison = test_models(X, y, groups, self.random_seed, self.nb_threads or -1)
        else:
            model, comparison = self.model, None

        estimator, estimation = estimate_model(
            X, y, groups, model, self.random_seed, self.nb_threads or -1
        )

        self.output["estimator"].write(estimator)
        results = estimation_results(
            y, groups, self.features, model, comparison, estimation, self.random_seed
        )
        self.output["results"].write(results)


class JointToursFeatureImportanceStep(RandomStep, ThreadedStep):
    """Computes the permutation feature importance of the joint-tour classifier."""

    feature_importance = BoolParameter(
        "joint_travel.feature_importance",
        default=False,
        description=(
            "Whether to compute a feature importance analysis of the joint-tour classifier."
        ),
    )

    input_files = {"tours": SurveyedToursFile, "estimator": JointTourEstimatorFile}
    output_files = {"importance": JointTourFeatureImportanceFile}

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

        logger.info("Feature importance of the joint-tour classifier (increase in Brier score):")
        for feature, importance, std in df.head(10).iter_rows():
            logger.info(f"  {feature}: {importance:.4f} (+/- {std:.4f})")

        self.output["importance"].write(df)


class JointToursFeatureImportancePlotStep(PlotStep):
    """Plots the permutation feature importance of the joint-tour classifier."""

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

    input_files = {"importance": JointTourFeatureImportanceFile}
    output_files = {"plot": JointTourFeatureImportancePlotFile}

    def run(self):
        df = self.input["importance"].read()
        fig = plot_feature_importance(
            df,
            "Increase in Brier score when the feature is shuffled",
            max_features=self.nb_plotted_features,
            labels=TOUR_VARIABLE_LABELS,
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


def restrict_to_home_in_area(df: pl.DataFrame, area) -> pl.DataFrame:
    """Restricts `df` to the rows whose home (columns `home_lng` and `home_lat`) falls within
    `area` (a shapely geometry in EPSG:4326). Rows without home coordinates are dropped."""
    import numpy as np
    import polars as pl
    import shapely

    lngs = df["home_lng"].cast(pl.Float64).fill_null(np.nan).to_numpy()
    lats = df["home_lat"].cast(pl.Float64).fill_null(np.nan).to_numpy()
    return df.filter(pl.Series(shapely.contains_xy(area, lngs, lats)))


def compute_joint_tours_shares(survey: pl.DataFrame, sim: pl.DataFrame) -> dict:
    """Computes the share of joint tours, by tour count and by distance, for the survey (weighted)
    and the simulation, together with the totals the shares are computed on."""
    survey_joint = survey.filter("joint_tour")
    survey_weight = float(survey["weight"].sum())
    survey_joint_weight = float(survey_joint["weight"].sum())
    survey_distance = float(survey["weighted_distance"].sum())
    survey_joint_distance = float(survey_joint["weighted_distance"].sum())

    sim_count = len(sim)
    sim_joint_count = int(sim["joint_tour"].sum())
    sim_distance: float | None
    sim_joint_distance: float | None
    if "total_distance" in sim.columns:
        sim_distance = float(sim["total_distance"].sum())
        sim_joint_distance = float(sim.filter("joint_tour")["total_distance"].sum())
    else:
        logger.warning(
            "`total_distance` is not available in `ToursFile`: skipping the distance-based "
            "joint-tour share comparison for the simulated population."
        )
        sim_distance = sim_joint_distance = None

    def ratio(num: float | None, den: float | None) -> float | None:
        return None if num is None or not den else num / den

    return {
        "tour_count": {
            "survey": ratio(survey_joint_weight, survey_weight),
            "simulation": ratio(sim_joint_count, sim_count),
        },
        "distance": {
            "survey": ratio(survey_joint_distance, survey_distance),
            "simulation": ratio(sim_joint_distance, sim_distance),
        },
        "totals": {
            "survey_weight": survey_weight,
            "simulation_tours": sim_count,
            "survey_distance": survey_distance,
            "simulation_distance": sim_distance,
        },
    }


class CompareJointToursShareStep(PlotStep, PopulationStep):
    """Compares the share of joint tours in the survey and in the simulated population, by tour
    count and by distance."""

    # TODO. This Step is only valid if the survey area is similar to the simulation area.

    input_files = {
        "survey_tours": SurveyedToursFile,
        "survey_households": SurveyedHouseholdsFile,
        "survey_zones": InputFile(SurveyedDrawZonesFile, optional=True),
        "simulation_area": InputFile(SimulationAreaFile, optional=True),
        "sim_tours": ToursFile,
        "sim_joint": JointToursFile,
        "sim_trips": TripsFile,
        "sim_homes": HouseholdsHomesFile,
    }
    output_files = {
        "comparison": JointToursShareComparisonFile,
        "tour_count_plot": JointToursShareTourCountPlotFile,
        "distance_plot": JointToursShareDistancePlotFile,
    }

    def run(self):
        import polars as pl

        survey = self.input["survey_tours"].read()
        # Only fully surveyed households (and on same day) have a reliable "joint" status.
        survey = survey.filter("household_fully_surveyed", "household_surveyed_on_same_day")
        survey = survey.drop_nulls(["joint_tour", "weight", "weighted_distance"])
        households = self.input["survey_households"].read()
        for col in ("home_lng", "home_lat", "home_draw_zone"):
            if col not in households.columns:
                raise MetropyError(f"Column `{col}` is missing from `SurveyedHouseholdsFile`.")
        survey = survey.join(
            households.select("household_id", "home_lng", "home_lat", "home_draw_zone"),
            on="household_id",
            how="left",
        )

        # The households of the simulated tours are read from the trips.
        tour_households = self.input["sim_trips"].scan().select("tour_id", "household_id").unique()
        homes = self.input["sim_homes"].read().to_crs("EPSG:4326")
        homes = pl.from_pandas(homes[["household_id"]]).with_columns(
            home_lng=pl.Series(homes.geometry.x.to_numpy()),
            home_lat=pl.Series(homes.geometry.y.to_numpy()),
        )
        sim = (
            self.input["sim_tours"]
            .read()
            .join(self.input["sim_joint"].read(), on="tour_id")
            .join(tour_households.collect(), on="tour_id", how="left")
            .join(homes, on="household_id", how="left")
        )

        # First, restrict the surveyed tours to those with home in the simulation area.
        if self.input["simulation_area"].exists():
            sim_area = self.input["simulation_area"].get_area("EPSG:4326")  # ty: ignore[unresolved-attribute]
            survey = restrict_to_home_in_area(survey, sim_area)

        # Then, restrict the simulated tours to those with home in the draw zones where at least
        # one surveyed tour is observed.
        if self.input["survey_zones"].exists():
            draw_zones = self.input["survey_zones"].read()
            observed = survey["home_draw_zone"].drop_nulls().unique().to_list()
            draw_zones = draw_zones[draw_zones["draw_zone_id"].isin(observed)]
            if draw_zones.empty:
                raise MetropyError("No draw zone with an observed tour in the simulation area.")
            survey_area = draw_zones.to_crs("EPSG:4326").union_all()
            sim = restrict_to_home_in_area(sim, survey_area)

        results = compute_joint_tours_shares(survey, sim)
        totals = results.pop("totals")

        logger.debug(
            f"Total tours after filtering: survey (weighted) {totals['survey_weight']:,.0f} "
            f"vs. simulation {totals['simulation_tours']:,}"
        )
        if totals["simulation_distance"] is not None:
            logger.debug(
                f"Total distance after filtering: survey (weighted) "
                f"{totals['survey_distance']:,.0f} m vs. simulation "
                f"{totals['simulation_distance']:,.0f} m"
            )

        self.output["comparison"].write(results)

        fig = plot_joint_share_comparison(
            results["tour_count"]["survey"] or 0.0,
            results["tour_count"]["simulation"] or 0.0,
            "Share of tours",
        )
        self.output["tour_count_plot"].write(fig)

        sim_dist = results["distance"]["simulation"]
        fig = plot_joint_share_comparison(
            results["distance"]["survey"] or 0.0, sim_dist or 0.0, "Share of distance"
        )
        self.output["distance_plot"].write(fig)
