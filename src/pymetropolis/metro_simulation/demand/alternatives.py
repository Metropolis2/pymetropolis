from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_common.errors import MetropyError, error_context
from pymetropolis.metro_common.utils import pl_duration_to_seconds
from pymetropolis.metro_demand.modes import MODE_PREFERENCES_FILES, OutsideOptionPreferencesFile
from pymetropolis.metro_demand.population import UniformDrawsFile
from pymetropolis.metro_demand.population.files import TripsFile
from pymetropolis.metro_demand.routing.files import PrimaryCarTripsAccessEgressFile
from pymetropolis.metro_pipeline import PopulationStep, Step
from pymetropolis.metro_pipeline.parameters import EnumParameter, FloatParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_simulation.common import StepWithModes, merge_populations

from .files import (
    MetroAlternativesFile,
    MetroAlternativesPopulationFile,
    MetroExAnteAlternativesFile,
    MetroExAnteAlternativesPopulationFile,
    MetroExAnteTripsPopulationFile,
    MetroTripsPopulationFile,
)

if TYPE_CHECKING:
    import polars as pl

    from pymetropolis.metro_pipeline.file import MetroDataFrameFile


@error_context(msg="Cannot generate departure-time columns of alternatives")
def generate_departure_time_columns(
    tour_ids: pl.Series,
    departure_time_choice_model: str,
    departure_time_choice_mu: float | None,
    draw_file: UniformDrawsFile,
):
    import polars as pl

    df = pl.DataFrame({"agent_id": tour_ids})
    if departure_time_choice_model == "ContinuousLogit":
        df = df.with_columns(
            pl.lit("Continuous").alias("dt_choice.type"),
            pl.lit("Logit").alias("dt_choice.model.type"),
            pl.lit(departure_time_choice_mu).alias("dt_choice.model.mu"),
        )
        draws: pl.DataFrame = draw_file.read()
        df = df.join(
            draws.select(pl.col("departure_time_u").alias("dt_choice.model.u"), agent_id="tour_id"),
            on="agent_id",
            how="left",
        )
    elif departure_time_choice_model == "Exogenous":
        raise MetropyError("TODO")
    return df


@error_context(msg="Cannot generate outside-option alternatives")
def generate_outside_option_alts(pref_file: OutsideOptionPreferencesFile):
    import polars as pl

    df: pl.DataFrame = pref_file.read()
    df = (
        df.rename({"tour_id": "agent_id"})
        .with_columns(
            alt_id=pl.lit("outside_option"), constant_utility=-pl.col("outside_option_cst")
        )
        .drop("outside_option_cst")
    )
    return df


@error_context(msg="Cannot generate the mode constants of alternatives")
def add_mode_constants(alts: pl.DataFrame, pref_files: dict[str, MetroDataFrameFile]):
    """Adds the tour-level mode constant to the utility of each (tour, mode) alternative.

    The constant is a penalty of traveling by that mode during the whole tour, so it is added once
    per alternative.
    """
    import polars as pl

    constants = pl.DataFrame()
    for mode, pref_file in pref_files.items():
        if pref_file is None or not pref_file.exists():
            continue
        df: pl.DataFrame = pref_file.read().select(
            agent_id="tour_id", alt_id=pl.lit(mode), constant_utility=-pl.col(f"{mode}_cst")
        )
        constants = pl.concat((constants, df), how="vertical")
    if constants.is_empty():
        return alts
    return alts.join(constants, on=["agent_id", "alt_id"], how="left")


def get_origin_delay(input_trips: pl.DataFrame, primary_car_trips: pl.DataFrame):
    import polars as pl

    # For primary car trips, `origin_delay` is equal to the access time of the first
    # trip in the tour.
    return (
        input_trips.select("agent_id", "alt_id", "trip_id")
        .filter(pl.col("alt_id").str.starts_with("car_"))
        .join(primary_car_trips.select("trip_id", "access_time"), on="trip_id", how="left")
        .group_by("agent_id", "alt_id")
        .agg(origin_delay=pl_duration_to_seconds(pl.col("access_time").first()).fill_null(0.0))
    )


class PrepareMetroAlternativesStep(StepWithModes, PopulationStep):
    """Prepares the alternatives for the Metropolis-Core simulation."""

    departure_time_choice_model = EnumParameter(
        "departure_time_choice.model",
        values=["ContinuousLogit", "Exogenous"],
        description="Type of choice model for departure-time choice",
    )
    departure_time_choice_mu = FloatParameter(
        "departure_time_choice.mu",
        default=1.0,
        description="Value of mu for the Continuous Logit departure-time choice model",
        note="Only required when departure-time choice model is ContinuousLogit",
    )
    input_files = {
        "input_trips": InputFile(
            MetroTripsPopulationFile,
            when=lambda inst: inst.has_trip_mode(),
            when_doc='if at least one "trip-based" mode is defined',
        ),
        "uniform_draws": InputFile(
            UniformDrawsFile,
            when=lambda inst: (
                inst.has_trip_mode() and inst.departure_time_choice_model == "ContinuousLogit"
            ),
            when_doc=(
                'if at least one "trip-based" mode is defined and departure-time choice '
                'is "ContinuousLogit"'
            ),
        ),
        "outside_option_preferences": InputFile(
            OutsideOptionPreferencesFile,
            when=lambda inst: inst.has_mode("outside_option"),
            when_doc="if the outside-option mode is defined",
        ),
        "primary_car_trips": InputFile(PrimaryCarTripsAccessEgressFile, optional=True),
        **{
            f"{mode}_preferences": InputFile(
                pref_file,
                optional=True,
                when=lambda inst, mode=mode: inst.has_mode(mode),
                when_doc=f'if the "{mode}" mode is defined',
            )
            for mode, pref_file in MODE_PREFERENCES_FILES.items()
        },
    }
    output_files = {"metro_alternatives": MetroAlternativesPopulationFile}

    def is_defined(self) -> bool:
        if self.modes is None or len(self.modes) == 0:
            return False
        # Step is NOT defined if there is a trip mode but the departure-time choice model is not
        # defined.
        return not self.has_trip_mode() or self.departure_time_choice_model is not None

    def run(self):
        import polars as pl

        input_trips = self.input["input_trips"].read_if_exists()
        primary_car_trips = self.input["primary_car_trips"].read_if_exists()
        alts = pl.DataFrame()
        if input_trips is not None:
            alts = input_trips.select("agent_id", "alt_id").unique()
            dep_time_df = generate_departure_time_columns(
                alts["agent_id"].unique(),
                self.departure_time_choice_model,
                self.departure_time_choice_mu,
                self.input["uniform_draws"],
            )
            alts = alts.join(dep_time_df, on="agent_id", how="left")
            if primary_car_trips is not None:
                origin_delays = get_origin_delay(input_trips, primary_car_trips)
                alts = alts.join(origin_delays, on=["agent_id", "alt_id"], how="left")
            alts = add_mode_constants(
                alts,
                {
                    mode: self.input[f"{mode}_preferences"]
                    for mode in MODE_PREFERENCES_FILES
                    if self.has_mode(mode)
                },
            )
        if self.has_mode("outside_option"):
            outside_option_alts = generate_outside_option_alts(
                self.input["outside_option_preferences"]
            )
            # There is no departure-time choice for the outside option alternative.
            alts = pl.concat((alts, outside_option_alts), how="diagonal")
        alts = alts.sort("agent_id", "alt_id")
        self.output["metro_alternatives"].write(alts)


class PrepareExAnteMetroAlternativesStep(StepWithModes, PopulationStep):
    """Prepares the alternatives for the ex-ante simulation."""

    input_files = {
        "input_trips": MetroExAnteTripsPopulationFile,
        "trips": TripsFile,
        "primary_car_trips": InputFile(PrimaryCarTripsAccessEgressFile, optional=True),
    }
    output_files = {"metro_alternatives": MetroExAnteAlternativesPopulationFile}
    priority = 0

    def run(self):
        import polars as pl

        trips = self.input["trips"].read()
        input_trips = self.input["input_trips"].read()
        primary_car_trips = self.input["primary_car_trips"].read_if_exists()

        assert input_trips["trip_id"].n_unique() == len(input_trips)
        assert (input_trips["agent_id"] == input_trips["trip_id"]).all()
        alts = (
            input_trips.select("agent_id", "alt_id", "trip_id")
            .join(trips.select("trip_id", "departure_time"), on="trip_id", how="left")
            .drop("trip_id")
        )
        alts = alts.with_columns(
            pl.lit("Constant").alias("dt_choice.type"),
            pl_duration_to_seconds("departure_time").alias("dt_choice.departure_time"),
        ).drop("departure_time")
        if primary_car_trips is not None:
            origin_delays = get_origin_delay(input_trips, primary_car_trips)
            alts = alts.join(origin_delays, on=["agent_id", "alt_id"], how="left")
        alts = alts.sort("agent_id", "alt_id")
        self.output["metro_alternatives"].write(alts)


class WriteMetroAlternativesStep(Step):
    """Merges the alternatives in each population and writes the alternatives input file for
    Metropolis-Core.
    """

    input_files = {
        "population_alts": InputFile(MetroAlternativesPopulationFile, all_populations=True)
    }
    output_files = {"metro_alts": MetroAlternativesFile}

    def run(self):
        alts = merge_populations(self.input_populations["population_alts"])
        self.output["metro_alts"].write(alts)


class WriteExAnteMetroAlternativesStep(Step):
    """Merges the alternatives in each population and writes the alternatives input file for the
    ex-ante simulation.
    """

    input_files = {
        "population_alts": InputFile(MetroExAnteAlternativesPopulationFile, all_populations=True)
    }
    output_files = {"metro_alts": MetroExAnteAlternativesFile}
    priority = 0

    def run(self):
        alts = merge_populations(self.input_populations["population_alts"])
        self.output["metro_alts"].write(alts)
