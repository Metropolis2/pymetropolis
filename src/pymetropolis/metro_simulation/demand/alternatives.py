import polars as pl

from pymetropolis.metro_common.errors import MetropyError, error_context
from pymetropolis.metro_common.types import FixedValues
from pymetropolis.metro_demand.modes import (
    CAR_DRIVER_TABLE,
    OUTSIDE_OPTION_PARAMETERS_FILE,
    OUTSIDE_OPTION_TABLE,
    PUBLIC_TRANSIT_TABLE,
)
from pymetropolis.metro_demand.population import TRIPS_FILE, UNIFORM_DRAWS_FILE
from pymetropolis.metro_pipeline import Config, ConfigTable, ConfigValue, Step

from .files import METRO_ALTS_FILE

DEPARTURE_TIME_CHOICE_MODEL = ConfigValue(
    "departure_time_choice.model",
    key="model",
    expected_type=FixedValues(["ContinuousLogit", "Exogenous"]),
    description="Type of choice model for departure-time choice",
)

DEPARTURE_TIME_CHOICE_MU = ConfigValue(
    "departure_time_choice.mu",
    key="mu",
    expected_type=float,
    default=1.0,
    description="Value of mu for the Continuous Logit departure-time choice model",
    note="Only required when departure-time choice model is ContinuousLogit",
)

DEPARTURE_TIME_CHOICE_TABLE = ConfigTable(
    "departure_time_choice",
    key="departure_time_choice",
    items=[DEPARTURE_TIME_CHOICE_MODEL, DEPARTURE_TIME_CHOICE_MU],
)


@error_context(msg="Cannot generate departure-time columns of alternatives")
def generate_departure_time_columns(tour_ids: pl.Series, config: Config):
    df = pl.DataFrame({"agent_id": tour_ids})
    model = config[DEPARTURE_TIME_CHOICE_MODEL]
    if model == "ContinuousLogit":
        df = df.with_columns(
            pl.lit("Continuous").alias("dt_choice.type"),
            pl.lit("Logit").alias("dt_choice.model.type"),
            pl.lit(config[DEPARTURE_TIME_CHOICE_MU]).alias("dt_choice.model.mu"),
        )
    elif model == "Exogenous":
        raise MetropyError("TODO")
    draws = UNIFORM_DRAWS_FILE.read(config)
    df = df.join(
        draws.select(pl.col("departure_time_u").alias("dt_choice.model.u"), agent_id="tour_id"),
        on="agent_id",
        how="left",
    )
    return df


@error_context(msg="Cannot generate car driver alternatives")
def generate_car_driver_alts(tour_ids: pl.Series, config: Config):
    df = pl.DataFrame({"agent_id": tour_ids, "alt_id": "car_driver"})
    return df


@error_context(msg="Cannot generate public-transit alternatives")
def generate_public_transit_alts(tour_ids: pl.Series, config: Config):
    df = pl.DataFrame({"agent_id": tour_ids, "alt_id": "public_transit"})
    return df


@error_context(msg="Cannot generate outside-option alternatives")
def generate_outside_option_alts(tour_ids: pl.Series, config: Config):
    df = pl.DataFrame({"agent_id": tour_ids, "alt_id": "outside_option"})
    # TODO. Manage outside option constant at the person vs tour level.
    constants = OUTSIDE_OPTION_PARAMETERS_FILE.read(config)
    df = (
        df.join(constants.rename({"tour_id": "agent_id"}), on="agent_id", how="left")
        .with_columns(constant_utility=pl.col("outside_option_cst"))
        .drop("outside_option_cst")
    )
    return df


@error_context(msg="Cannot write alternatives file")
def write_alternatives(config: Config):
    trips = TRIPS_FILE.read(config)
    tour_ids = trips["tour_id"].unique().sort()
    dep_time_df = generate_departure_time_columns(tour_ids, config)
    alts = pl.DataFrame()
    if config.has_table(CAR_DRIVER_TABLE):
        car_driver_alts = generate_car_driver_alts(tour_ids, config)
        car_driver_alts = car_driver_alts.join(dep_time_df, on="agent_id", how="left")
        alts = pl.concat((alts, car_driver_alts), how="diagonal")
        # TODO. Add alternative-level constant utility
    if config.has_table(PUBLIC_TRANSIT_TABLE):
        public_transit_alts = generate_public_transit_alts(tour_ids, config)
        public_transit_alts = public_transit_alts.join(dep_time_df, on="agent_id", how="left")
        alts = pl.concat((alts, public_transit_alts), how="diagonal")
    if config.has_table(OUTSIDE_OPTION_TABLE):
        outside_option_alts = generate_outside_option_alts(tour_ids, config)
        # There is no departure-time choice for the outside option alternative.
        alts = pl.concat((alts, outside_option_alts), how="diagonal")
    METRO_ALTS_FILE.save(alts, config)
    return True


WRITE_ALTERNATIVES_STEP = Step(
    "write-alternatives",
    write_alternatives,
    required_files=[TRIPS_FILE, UNIFORM_DRAWS_FILE],
    optional_files=[OUTSIDE_OPTION_PARAMETERS_FILE],
    output_files=[METRO_ALTS_FILE],
    config_values=[DEPARTURE_TIME_CHOICE_MODEL, DEPARTURE_TIME_CHOICE_MU],
    # TODO. Add optional config values
)
