from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_common.errors import MetropyError, error_context
from pymetropolis.metro_demand.modes import OutsideOptionPreferencesFile
from pymetropolis.metro_demand.population import UniformDrawsFile
from pymetropolis.metro_pipeline.parameters import EnumParameter, FloatParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_simulation.common import StepWithModes

from .files import MetroAlternativesFile, MetroTripsFile

if TYPE_CHECKING:
    import polars as pl


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

    # TODO. Manage outside option constant at the person vs tour level.
    df: pl.DataFrame = pref_file.read()
    df = (
        df.rename({"tour_id": "agent_id"})
        .with_columns(alt_id="outside_option", constant_utility=-pl.col("outside_option_cst"))
        .drop("outside_option_cst")
    )
    return df


class WriteMetroAlternativesStep(StepWithModes):
    """Generates the input alternatives file for the Metropolis-Core simulation."""

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
            MetroTripsFile,
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
    }
    output_files = {"metro_alternatives": MetroAlternativesFile}

    def is_defined(self) -> bool:
        if self.modes is None or len(self.modes) == 0:
            return False
        # Step is NOT defined if there is a trip mode but the departure-time choice model is not
        # defined.
        return not self.has_trip_mode() or self.departure_time_choice_model is not None

    def run(self):
        import polars as pl

        input_trips: pl.DataFrame | None = self.input["input_trips"].read_if_exists()
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
        if self.has_mode("outside_option"):
            outside_option_alts = generate_outside_option_alts(
                self.input["outside_option_preferences"]
            )
            # There is no departure-time choice for the outside option alternative.
            alts = pl.concat((alts, outside_option_alts), how="diagonal")
        alts = alts.sort("agent_id", "alt_id")
        self.output["metro_alternatives"].write(alts)
