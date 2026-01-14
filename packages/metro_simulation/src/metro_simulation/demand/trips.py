import polars as pl
from metro_common.errors import MetropyError, error_context
from metro_common.utils import time_to_seconds_since_midnight_pl
from metro_demand.departure_time import LINEAR_SCHEDULE_FILE, TSTAR_FILE
from metro_demand.modes import (
    CAR_DRIVER_ODS_FILE,
    CAR_DRIVER_PARAMETERS_FILE,
    CAR_DRIVER_TABLE,
    PUBLIC_TRANSIT_PARAMETERS_FILE,
    PUBLIC_TRANSIT_TABLE,
    PUBLIC_TRANSIT_TRAVEL_TIMES_FILE,
)
from metro_demand.population import TRIPS_FILE
from metro_pipeline import Config, Step

from .files import METRO_TRIPS_FILE


@error_context(msg="Cannot generate car driver trips")
def generate_car_driver_trips(df: pl.DataFrame, config: Config):
    df = df.with_columns(
        pl.lit("car_driver").alias("alt_id"),
        pl.lit("Road").alias("class.type"),
        pl.lit("car_driver").alias("class.vehicle"),
    )
    ods = CAR_DRIVER_ODS_FILE.read_if_exists(config)
    if ods is None:
        raise MetropyError("Missing required file: `CAR_DRIVER_ODS_FILE`")
    else:
        df = (
            df.join(ods, on="trip_id", how="left")
            .with_columns(
                pl.col("origin_node_id").alias("class.origin"),
                pl.col("destination_node_id").alias("class.destination"),
            )
            .drop("origin_node_id", "destination_node_id")
        )
    params = CAR_DRIVER_PARAMETERS_FILE.read_if_exists(config)
    if params is not None:
        df = (
            df.join(params, on="person_id", how="left")
            .with_columns(
                constant_utility=-pl.col("car_driver_cst"),
                alpha=pl.col("car_driver_vot") / 3600.0,
            )
            .drop("car_driver_cst", "car_driver_vot")
        )
    tstars = TSTAR_FILE.read_if_exists(config)
    if tstars is not None:
        df = (
            df.join(tstars, on="trip_id", how="left")
            .with_columns(
                time_to_seconds_since_midnight_pl(pl.col("tstar")).alias("schedule_utility.tstar")
            )
            .drop("tstar")
        )
    params = LINEAR_SCHEDULE_FILE.read_if_exists(config)
    if params is not None:
        df = (
            df.join(params, on="trip_id", how="left")
            .with_columns(
                pl.lit("Linear").alias("schedule_utility.type"),
                (pl.col("beta") / 3600.0).alias("schedule_utility.beta"),
                (pl.col("gamma") / 3600.0).alias("schedule_utility.gamma"),
                pl.col("delta").dt.total_seconds().cast(pl.Float64).alias("schedule_utility.delta"),
            )
            .drop("beta", "gamma", "delta")
        )
    return df


@error_context(msg="Cannot generate public-transit trips")
def generate_public_transit_trips(df: pl.DataFrame, config: Config):
    df = df.with_columns(
        pl.lit("public_transit").alias("alt_id"),
        pl.lit("Virtual").alias("class.type"),
    )
    tts = PUBLIC_TRANSIT_TRAVEL_TIMES_FILE.read_if_exists(config)
    if tts is None:
        raise MetropyError("Missing required file: `PUBLIC_TRANSIT_TRAVEL_TIMES_FILE`")
    else:
        df = (
            df.join(tts, on="trip_id", how="left")
            .with_columns(
                pl.col("public_transit_travel_time")
                .dt.total_seconds()
                .cast(pl.Float64)
                .alias("class.travel_time")
            )
            .drop("public_transit_travel_time")
        )
    params = PUBLIC_TRANSIT_PARAMETERS_FILE.read_if_exists(config)
    if params is not None:
        df = (
            df.join(params, on="person_id", how="left")
            .with_columns(
                constant_utility=-pl.col("public_transit_cst"),
                alpha=pl.col("public_transit_vot") / 3600.0,
            )
            .drop("public_transit_cst", "public_transit_vot")
        )
    tstars = TSTAR_FILE.read_if_exists(config)
    if tstars is not None:
        df = (
            df.join(tstars, on="trip_id", how="left")
            .with_columns(
                time_to_seconds_since_midnight_pl(pl.col("tstar")).alias("schedule_utility.tstar")
            )
            .drop("tstar")
        )
    params = LINEAR_SCHEDULE_FILE.read_if_exists(config)
    if params is not None:
        df = (
            df.join(params, on="trip_id", how="left")
            .with_columns(
                pl.lit("Linear").alias("schedule_utility.type"),
                (pl.col("beta") / 3600.0).alias("schedule_utility.beta"),
                (pl.col("gamma") / 3600.0).alias("schedule_utility.gamma"),
                pl.col("delta").dt.total_seconds().cast(pl.Float64).alias("schedule_utility.delta"),
            )
            .drop("beta", "gamma", "delta")
        )
    return df


@error_context(msg="Cannot write trips file")
def write_trips(config: Config):
    trips = TRIPS_FILE.read(config)
    df = trips.select("trip_id", "person_id", agent_id="tour_id").sort("agent_id", "trip_id")
    metro_trips = pl.DataFrame()
    if config.has_table(CAR_DRIVER_TABLE):
        car_driver_trips = generate_car_driver_trips(df, config)
        metro_trips = pl.concat((metro_trips, car_driver_trips), how="diagonal")
    if config.has_table(PUBLIC_TRANSIT_TABLE):
        public_transit_trips = generate_public_transit_trips(df, config)
        metro_trips = pl.concat((metro_trips, public_transit_trips), how="diagonal")
    metro_trips = metro_trips.drop("person_id")
    METRO_TRIPS_FILE.save(metro_trips, config)
    return True


WRITE_TRIPS_STEP = Step(
    "write-trips",
    write_trips,
    required_files=[TRIPS_FILE],
    optional_files=[
        CAR_DRIVER_PARAMETERS_FILE,
        CAR_DRIVER_ODS_FILE,
        PUBLIC_TRANSIT_PARAMETERS_FILE,
        PUBLIC_TRANSIT_TRAVEL_TIMES_FILE,
        LINEAR_SCHEDULE_FILE,
        TSTAR_FILE,
    ],
    output_files=[METRO_TRIPS_FILE],
)
