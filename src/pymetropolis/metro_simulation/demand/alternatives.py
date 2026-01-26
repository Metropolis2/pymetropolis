from typing import Optional

import polars as pl

from pymetropolis.metro_common.errors import MetropyError, error_context
from pymetropolis.metro_demand.modes import (
    OutsideOptionPreferencesFile,
)
from pymetropolis.metro_demand.population import TripsFile, UniformDrawsFile
from pymetropolis.metro_pipeline.parameters import EnumParameter, FloatParameter

from .common import MetroStepWithModes
from .files import MetroAlternativesFile


@error_context(msg="Cannot generate departure-time columns of alternatives")
def generate_departure_time_columns(
    tour_ids: pl.Series,
    departure_time_choice_model: str,
    departure_time_choice_mu: Optional[float],
    draw_file: UniformDrawsFile,
):
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


@error_context(msg="Cannot generate car driver alternatives")
def generate_car_driver_alts(tour_ids: pl.Series):
    df = pl.DataFrame({"agent_id": tour_ids, "alt_id": "car_driver"})
    return df


@error_context(msg="Cannot generate public-transit alternatives")
def generate_public_transit_alts(tour_ids: pl.Series):
    df = pl.DataFrame({"agent_id": tour_ids, "alt_id": "public_transit"})
    return df


@error_context(msg="Cannot generate outside-option alternatives")
def generate_outside_option_alts(tour_ids: pl.Series, pref_file: OutsideOptionPreferencesFile):
    df = pl.DataFrame({"agent_id": tour_ids, "alt_id": "outside_option"})
    # TODO. Manage outside option constant at the person vs tour level.
    constants: pl.DataFrame = pref_file.read()
    df = (
        df.join(constants.rename({"tour_id": "agent_id"}), on="agent_id", how="left")
        .with_columns(constant_utility=pl.col("outside_option_cst"))
        .drop("outside_option_cst")
    )
    return df


class WriteMetroAlternativesStep(MetroStepWithModes):
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
    output_files = {"metro_alternatives": MetroAlternativesFile}

    def is_defined(self) -> bool:
        if self.modes is None:
            return False
        if self.has_trip_modes() and self.departure_time_choice_model is None:
            return False
        return True

    def required_files(self):
        files = dict()
        if self.has_trip_modes():
            files["trips"] = TripsFile
            if self.departure_time_choice_model == "ContinuousLogit":
                files["uniform_draws"] = UniformDrawsFile
        if self.has_mode("outside_option"):
            files["outside_option_preferences"] = OutsideOptionPreferencesFile
        return files

    def run(self):
        trips: pl.DataFrame = self.input["trips"].read()
        tour_ids = trips["tour_id"].unique().sort()
        if self.has_trip_modes():
            dep_time_df = generate_departure_time_columns(
                tour_ids,
                self.departure_time_choice_model,
                self.departure_time_choice_mu,
                self.input["uniform_draws"],
            )
        alts = pl.DataFrame()
        if self.has_mode("car_driver"):
            car_driver_alts = generate_car_driver_alts(tour_ids)
            car_driver_alts = car_driver_alts.join(dep_time_df, on="agent_id", how="left")
            alts = pl.concat((alts, car_driver_alts), how="diagonal")
            # TODO. Add alternative-level constant utility
        if self.has_mode("public_transit"):
            public_transit_alts = generate_public_transit_alts(tour_ids)
            public_transit_alts = public_transit_alts.join(dep_time_df, on="agent_id", how="left")
            alts = pl.concat((alts, public_transit_alts), how="diagonal")
        if self.has_mode("outside_option"):
            outside_option_alts = generate_outside_option_alts(
                tour_ids, self.input["outside_option_preferences"]
            )
            # There is no departure-time choice for the outside option alternative.
            alts = pl.concat((alts, outside_option_alts), how="diagonal")
        self.output["metro_alternatives"].write(alts)
