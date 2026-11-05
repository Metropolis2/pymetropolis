from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.utils import pl_duration_to_seconds
from pymetropolis.metro_demand.population.files import (
    ExAnteArrivalTimesFile,
    ExAnteDepartureTimesFile,
    HouseholdsFile,
    HouseholdsHomesFile,
    HouseholdsHomesUrbanTypeFile,
    PersonsFile,
    PopulationCharacteristicsFile,
    ToursFile,
    TripsDistancesFile,
    TripsFile,
    TripsUrbanTypeFile,
)
from pymetropolis.metro_pipeline import PopulationStep, Step
from pymetropolis.metro_pipeline.parameters import BoolParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_spatial.simulation_area.file import SimulationAreaFile

from .common import restrict_to_home_in_area
from .files import (
    SurveyCharacteristicsFile,
    SurveyedDrawZonesFile,
    SurveyedHouseholdsFile,
    SurveyedPersonsFile,
    SurveyedToursFile,
    SurveyedTripsFile,
)
from .tours import professional_activity_expr, socioprofessional_class_expr, urban_type_exprs

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    import polars as pl

QUANTILES = {"min": 0.0, "q1": 0.25, "median": 0.5, "q3": 0.75, "max": 1.0}
DISTRIBUTION = (*QUANTILES, "mean")
VEHICLES = ("total", "mean", "share_positive")
URBAN_VARIABLES = ("density", "urban_type", "functional_area_type", "functional_area_category")

HOUSEHOLD_NUMERIC = {
    "nb_cars": VEHICLES,
    "nb_motorcycles": VEHICLES,
    "nb_bicycles": VEHICLES,
    "nb_persons": ("mean",),
    "nb_driving_licenses": ("mean",),
}
HOUSEHOLD_CATEGORIES = [f"home_{variable}" for variable in URBAN_VARIABLES]
PERSON_NUMERIC = {
    "woman": ("share",),
    "age": DISTRIBUTION,
    "has_driving_license": ("share",),
    "has_public_transit_subscription": ("share",),
    "nb_trips": ("mean",),
}
PERSON_CATEGORIES = ["education_level", "professional_activity", "socioprofessional_class"]
TRIP_NUMERIC = {
    "od_distance": ("total", *DISTRIBUTION),
    "departure_time": DISTRIBUTION,
    "arrival_time": DISTRIBUTION,
}
TRIP_CATEGORIES = [
    "origin_purpose_group",
    "destination_purpose_group",
    *(f"{side}_{v}" for side in ("origin", "destination") for v in URBAN_VARIABLES),
]
TOUR_NUMERIC = {"nb_trips": ("mean",)}


def weighted_quantiles(
    values: pl.Series, weights: pl.Series, quantiles: Sequence[float]
) -> list[float]:
    """Returns the weighted quantiles of `values`, using the inverted cumulative distribution
    function: each quantile is the smallest value whose cumulative weight share reaches it.

    The weights must be positive.
    """
    import numpy as np

    order = values.arg_sort()
    sorted_values = values.gather(order).to_numpy()
    cumulative_weights = weights.gather(order).cum_sum().to_numpy()
    indices = np.searchsorted(cumulative_weights, np.array(quantiles) * cumulative_weights[-1])
    return sorted_values[np.minimum(indices, len(sorted_values) - 1)].tolist()


def numeric_values(df: pl.DataFrame, column: str, weight: str = "weight") -> pl.DataFrame:
    """Returns the non-null values of `column` (as floats, in seconds for durations) with a
    positive weight, in columns `value` and `weight`.
    """
    import polars as pl

    value = pl.col(column)
    if df.schema[column] == pl.Duration:
        value = pl_duration_to_seconds(value)
    return (
        df.select(value=value.cast(pl.Float64).fill_nan(None), weight=pl.col(weight))
        .drop_nulls()
        .filter(pl.col("weight") > 0)
    )


def numeric_statistics(
    df: pl.DataFrame, column: str, statistics: Sequence[str], weight: str = "weight"
) -> dict[str, float] | None:
    """Returns the given weighted statistics of a numeric or boolean column.

    Returns `None` if the column is missing or has no valid value.
    """
    if column not in df.columns:
        return None
    values = numeric_values(df, column, weight)
    if values.is_empty():
        return None
    total_weight = float(values["weight"].sum())
    total = float((values["value"] * values["weight"]).sum())
    results = {"total": total, "mean": total / total_weight, "share": total / total_weight}
    if "share_positive" in statistics:
        positive_weight = float(values.filter(values["value"] > 0)["weight"].sum())
        results["share_positive"] = positive_weight / total_weight
    quantiles = [k for k in QUANTILES if k in statistics]
    if quantiles:
        levels = [QUANTILES[k] for k in quantiles]
        results.update(
            zip(quantiles, weighted_quantiles(values["value"], values["weight"], levels))
        )
    return {statistic: results[statistic] for statistic in statistics}


def categorical_statistics(df: pl.DataFrame, column: str) -> dict[str, dict[str, float]] | None:
    """Returns the weighted counts and shares of each non-null value of a column.

    Returns `None` if the column is missing or has no valid value.
    """
    import polars as pl

    if column not in df.columns:
        return None
    counts = (
        df.select(value=pl.col(column).cast(pl.String), weight="weight")
        .drop_nulls()
        .filter(pl.col("weight") > 0)
        .group_by("value")
        .agg(pl.col("weight").sum())
        .sort("value")
    )
    if counts.is_empty():
        return None
    total_weight = float(counts["weight"].sum())
    return {
        "counts": dict(zip(counts["value"], counts["weight"])),
        "shares": {k: w / total_weight for k, w in zip(counts["value"], counts["weight"])},
    }


def activity_durations(trips: pl.DataFrame) -> dict[str, dict[str, float | None]] | None:
    """Returns the total duration of the activities for each purpose, and its share over all
    purposes.

    Returns `None` if the columns are missing or there is no valid value.
    """
    import polars as pl

    if not {"destination_purpose_group", "destination_activity_duration"}.issubset(trips.columns):
        return None
    durations = (
        trips.select(
            purpose=pl.col("destination_purpose_group").cast(pl.String),
            duration=pl_duration_to_seconds("destination_activity_duration"),
            weight="weight",
        )
        .drop_nulls()
        .filter(pl.col("weight") > 0)
        .group_by("purpose")
        .agg(total=(pl.col("duration") * pl.col("weight")).sum())
        .sort("purpose")
    )
    if durations.is_empty():
        return None
    total = float(durations["total"].sum())
    return {
        purpose: {"total": value, "share": value / total if total else None}
        for purpose, value in zip(durations["purpose"], durations["total"])
    }


def with_weight(df: pl.DataFrame, column: str, section: str) -> pl.DataFrame:
    """Sets `column` to a unit weight if it is missing or if all its values are null."""
    import polars as pl

    if column in df.columns and df[column].null_count() == len(df) and len(df) > 0:
        logger.warning(f"Column `{column}` is null for all {section}: using unit weights.")
    elif column in df.columns:
        return df
    return df.with_columns(pl.lit(1.0).alias(column))


def section_statistics(
    df: pl.DataFrame,
    section: str,
    numeric: Mapping[str, Sequence[str]],
    categories: Sequence[str] = (),
    column_weights: dict[str, str] | None = None,
    with_activity_durations: bool = False,
) -> dict:
    """Returns the statistics of a DataFrame, leaving out the variables with no valid value.

    `sample_count` is only returned for DataFrames with a `weight` column (i.e., for the survey).
    `column_weights` gives the columns for which another weight column should be used, when that
    column exists.
    """
    results: dict = {}
    if "weight" in df.columns:
        results["sample_count"] = len(df)
    df = with_weight(df, "weight", section)
    column_weights = {k: w for k, w in (column_weights or {}).items() if w in df.columns}
    for weight in set(column_weights.values()):
        df = with_weight(df, weight, section)
    results["count"] = float(df["weight"].sum())
    for column, statistics in numeric.items():
        values = numeric_statistics(df, column, statistics, column_weights.get(column, "weight"))
        if values is not None:
            results[column] = values
    for column in categories:
        values = categorical_statistics(df, column)
        if values is not None:
            results[column] = values
    if with_activity_durations:
        durations = activity_durations(df)
        if durations is not None:
            results["activity_durations"] = durations
    return results


def compute_characteristics(
    households: pl.DataFrame | None,
    persons: pl.DataFrame | None,
    trips: pl.DataFrame | None,
    tours: pl.DataFrame | None,
) -> dict:
    """Returns the characteristics of a population, leaving out the sections with no DataFrame.

    Each DataFrame can have a `weight` column (statistical weight of the observations, all
    observations have the same weight otherwise). `persons` can also have a `surveyed_weight`
    column, used for the variables that are only known for the persons surveyed for trips.
    """
    results = {}
    if households is not None:
        results["households"] = section_statistics(
            households, "households", HOUSEHOLD_NUMERIC, HOUSEHOLD_CATEGORIES
        )
    if persons is not None:
        results["persons"] = section_statistics(
            persons,
            "persons",
            PERSON_NUMERIC,
            PERSON_CATEGORIES,
            column_weights={"nb_trips": "surveyed_weight"},
        )
    if trips is not None:
        results["trips"] = section_statistics(
            trips, "trips", TRIP_NUMERIC, TRIP_CATEGORIES, with_activity_durations=True
        )
    if tours is not None:
        results["tours"] = section_statistics(tours, "tours", TOUR_NUMERIC)
    return results


def standardize_survey(
    households: pl.DataFrame, persons: pl.DataFrame, trips: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Converts survey households, persons and trips (in the MobiSurvStd format) to the variables
    of the simulated population, with their statistical weights.
    """
    import polars as pl

    licenses = pl.col("has_driving_license")
    nb_licenses = persons.group_by("household_id").agg(
        # NULL if the variable is unknown for the whole household.
        nb_driving_licenses=pl.when(licenses.is_not_null().any()).then(licenses.eq("yes").sum())
    )
    households = households.with_columns(**urban_type_exprs("home"), weight="sample_weight").join(
        nb_licenses, on="household_id", how="left"
    )
    trips = trips.join(
        persons.select("person_id", weight="sample_weight_surveyed"), on="person_id", how="left"
    ).with_columns(
        **urban_type_exprs("origin"),
        **urban_type_exprs("destination"),
        od_distance=pl.col("trip_euclidean_distance_km") * 1000,  # Convert to meters.
        departure_time=pl.duration(minutes="departure_time"),
        arrival_time=pl.duration(minutes="arrival_time"),
        destination_activity_duration=pl.duration(minutes="destination_activity_duration"),
    )
    persons = persons.with_columns(
        professional_activity=professional_activity_expr(),
        socioprofessional_class=socioprofessional_class_expr(),
        has_driving_license=licenses.eq("yes"),
        weight="sample_weight_all",
        surveyed_weight="sample_weight_surveyed",
    )
    return households, persons, trips


class SurveyCharacteristicsStep(Step):
    """Computes the characteristics of the households, persons, trips and tours in the survey.

    The characteristics are weighted with the survey weights.
    """

    restrict_to_simulation_area = BoolParameter(
        "survey.characteristics.restrict_to_simulation_area",
        default=False,
        description=(
            "If true, only the households whose home is in the simulation area are considered "
            "(along with their persons, trips and tours)."
        ),
    )
    input_files = {
        "households": SurveyedHouseholdsFile,
        "persons": SurveyedPersonsFile,
        "trips": SurveyedTripsFile,
        "tours": SurveyedToursFile,
        "simulation_area": InputFile(
            SimulationAreaFile,
            when=lambda step: step.restrict_to_simulation_area,
            when_doc="when `survey.characteristics.restrict_to_simulation_area` is true",
        ),
    }
    output_files = {"characteristics": SurveyCharacteristicsFile}

    def run(self):
        households = self.input["households"].read()
        persons = self.input["persons"].read()
        trips = self.input["trips"].read()
        tours = self.input["tours"].read()

        if self.restrict_to_simulation_area:
            for col in ("home_lng", "home_lat"):
                if col not in households.columns:
                    raise MetropyError(f"Column `{col}` is missing from `SurveyedHouseholdsFile`.")
            area = self.input["simulation_area"].get_area("EPSG:4326")  # ty: ignore[unresolved-attribute]
            households = restrict_to_home_in_area(households, area)
            if households.is_empty():
                raise MetropyError("No surveyed household has its home in the simulation area.")
            ids = households.select("household_id")
            persons = persons.join(ids, on="household_id", how="semi")
            trips = trips.join(ids, on="household_id", how="semi")
            tours = tours.join(ids, on="household_id", how="semi")

        households, persons, trips = standardize_survey(households, persons, trips)
        self.output["characteristics"].write(
            compute_characteristics(households, persons, trips, tours)
        )


class PopulationCharacteristicsStep(PopulationStep):
    """Computes the characteristics of the households, persons, trips and tours of the simulated
    population.

    Characteristics are left out when the corresponding file or column is not available.
    """

    restrict_to_survey_area = BoolParameter(
        "population_characteristics.restrict_to_survey_area",
        default=False,
        description=(
            "If true, only the households whose home is in a survey draw zone with at least one "
            "surveyed household are considered (along with their persons, trips and tours)."
        ),
    )
    input_files = {
        "trips": TripsFile,
        "households": InputFile(HouseholdsFile, optional=True),
        "persons": InputFile(PersonsFile, optional=True),
        "tours": InputFile(ToursFile, optional=True),
        "departure_times": InputFile(ExAnteDepartureTimesFile, optional=True),
        "arrival_times": InputFile(ExAnteArrivalTimesFile, optional=True),
        "distances": InputFile(TripsDistancesFile, optional=True),
        "home_urban_types": InputFile(HouseholdsHomesUrbanTypeFile, optional=True),
        "trip_urban_types": InputFile(TripsUrbanTypeFile, optional=True),
        "homes": InputFile(
            HouseholdsHomesFile,
            when=lambda step: step.restrict_to_survey_area,
            when_doc="when `population_characteristics.restrict_to_survey_area` is true",
        ),
        "survey_households": InputFile(
            SurveyedHouseholdsFile,
            when=lambda step: step.restrict_to_survey_area,
            when_doc="when `population_characteristics.restrict_to_survey_area` is true",
        ),
        "survey_zones": InputFile(
            SurveyedDrawZonesFile,
            when=lambda step: step.restrict_to_survey_area,
            when_doc="when `population_characteristics.restrict_to_survey_area` is true",
        ),
    }
    output_files = {"characteristics": PopulationCharacteristicsFile}

    def survey_area_household_ids(self) -> pl.DataFrame:
        """Returns the ids of the simulated households with home in the survey area."""
        import polars as pl

        if not self.input["survey_zones"].exists():
            raise MetropyError("`SurveyedDrawZonesFile` is required to find the survey area.")
        survey_households = self.input["survey_households"].read()
        if "home_draw_zone" not in survey_households.columns:
            raise MetropyError("Column `home_draw_zone` is missing from `SurveyedHouseholdsFile`.")
        observed = survey_households["home_draw_zone"].drop_nulls().unique().to_list()
        draw_zones = self.input["survey_zones"].read()
        draw_zones = draw_zones[draw_zones["draw_zone_id"].isin(observed)]
        if draw_zones.empty:
            raise MetropyError("No draw zone with an observed household in the survey.")
        survey_area = draw_zones.to_crs("EPSG:4326").union_all()

        homes = self.input["homes"].read().to_crs("EPSG:4326")
        homes = pl.from_pandas(homes[["household_id"]]).with_columns(
            home_lng=pl.Series(homes.geometry.x.to_numpy()),
            home_lat=pl.Series(homes.geometry.y.to_numpy()),
        )
        ids = restrict_to_home_in_area(homes, survey_area).select("household_id")
        if ids.is_empty():
            raise MetropyError("No simulated household has its home in the survey area.")
        return ids

    def run(self):
        households = self.input["households"].read_if_exists()
        persons = self.input["persons"].read_if_exists()
        trips = self.input["trips"].read()
        tours = self.input["tours"].read_if_exists()

        if self.restrict_to_survey_area:
            ids = self.survey_area_household_ids()
            trips = trips.join(ids, on="household_id", how="semi")
            if households is not None:
                households = households.join(ids, on="household_id", how="semi")
            if persons is not None:
                persons = persons.join(ids, on="household_id", how="semi")
            if tours is not None:
                tours = tours.join(trips.select("tour_id"), on="tour_id", how="semi")

        for name in ("departure_times", "arrival_times", "distances", "trip_urban_types"):
            df = self.input[name].read_if_exists()
            if df is not None:
                trips = trips.join(df, on="trip_id", how="left")
        home_urban_types = self.input["home_urban_types"].read_if_exists()
        if households is not None and home_urban_types is not None:
            households = households.join(home_urban_types, on="household_id", how="left")

        self.output["characteristics"].write(
            compute_characteristics(households, persons, trips, tours)
        )
