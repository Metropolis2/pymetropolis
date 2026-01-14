from datetime import timedelta

import numpy as np
import polars as pl
from metro_common.errors import error_context
from metro_common.types import Duration, FixedValues, Time
from metro_common.utils import seconds_since_midnight_to_time_pl, time_to_seconds_since_midnight
from metro_pipeline import RANDOM_SEED, Config, ConfigTable, ConfigValue, Step

from metro_demand.population import TRIPS_FILE

from .files import LINEAR_SCHEDULE_FILE, TSTAR_FILE

BETA = ConfigValue(
    "departure_time.linear_schedule.beta",
    key="beta",
    default=0.0,
    expected_type=float,
    description="Penalty for starting an activity earlier than the desired time (€/h).",
)

GAMMA = ConfigValue(
    "departure_time.linear_schedule.gamma",
    key="gamma",
    default=0.0,
    expected_type=float,
    description="Penalty for starting an activity later than the desired time (€/h).",
)

DELTA = ConfigValue(
    "departure_time.linear_schedule.delta",
    key="delta",
    default=timedelta(seconds=0),
    expected_type=Duration(),
    description="Length of the desired time window.",
)

TSTAR = ConfigValue(
    "departure_time.linear_schedule.tstar",
    key="tstar",
    expected_type=Time(),
    description="Desired start time of the following activity.",
)

TSTAR_STD = ConfigValue(
    "departure_time.linear_schedule.tstar_std",
    key="tstar_std",
    expected_type=Duration(),
    default=timedelta(seconds=0),
    description="Standard deviation of tstar.",
    note="For a uniform distribution, this is half the interval instead.",
)

TSTAR_DISTR = ConfigValue(
    "departure_time.linear_schedule.tstar_distr",
    key="tstar_distr",
    expected_type=FixedValues(["Uniform", "Normal"]),
    default="Uniform",
    description="Distribution of tstar.",
)

LINEAR_SCHEDULE_TABLE = ConfigTable(
    "departure_time.linear_schedule",
    "linear_schedule",
    items=[BETA, GAMMA, DELTA, TSTAR, TSTAR_STD, TSTAR_DISTR],
)


@error_context(msg="Cannot generate line schedule preferences.")
def generate_parameters(config: Config):
    trips = TRIPS_FILE.read(config)
    df = trips.select(
        "trip_id",
        beta=pl.lit(config[BETA]),
        gamma=pl.lit(config[GAMMA]),
        delta=pl.lit(config[DELTA]),
    )
    LINEAR_SCHEDULE_FILE.save(df, config)
    return True


LINEAR_SCHEDULE_STEP = Step(
    "linear-schedule",
    generate_parameters,
    required_files=[TRIPS_FILE],
    output_files=[LINEAR_SCHEDULE_FILE],
    config_values=[BETA, GAMMA, DELTA],
)


@error_context(msg="Cannot generate desired activity start times.")
def generate_tstars(config: Config):
    trips = TRIPS_FILE.read(config)
    tstar = config[TSTAR]
    std = config[TSTAR_STD]
    distr = config[TSTAR_DISTR]
    if std > timedelta(seconds=0):
        rng = np.random.default_rng(config[RANDOM_SEED])
        tstar_float = time_to_seconds_since_midnight(tstar)
        std_float = std.total_seconds()
        if distr == "Uniform":
            tstars = rng.uniform(tstar_float - std_float, tstar_float + std_float, size=len(trips))
        elif distr == "Normal":
            tstars = rng.normal(tstar_float, scale=std_float, size=len(trips))
        df = trips.select("trip_id", tstar=pl.Series(tstars)).with_columns(
            tstar=seconds_since_midnight_to_time_pl("tstar")
        )
    else:
        df = trips.select("trip_id", tstar=pl.lit(config[TSTAR]))
    TSTAR_FILE.save(df, config)
    return True


CONSTANT_TSTAR_STEP = Step(
    "constant-tstar",
    generate_tstars,
    required_files=[TRIPS_FILE],
    output_files=[TSTAR_FILE],
    config_values=[TSTAR],
)
