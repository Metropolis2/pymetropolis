import polars as pl

from pymetropolis.metro_common.errors import error_context
from pymetropolis.metro_demand.population import TRIPS_FILE
from pymetropolis.metro_pipeline import Config, ConfigTable, ConfigValue, Step

from .files import (
    CAR_DRIVER_DISTANCES_FILE,
    OUTSIDE_OPTION_PARAMETERS_FILE,
    OUTSIDE_OPTION_TRAVEL_TIMES_FILE,
)

CONSTANT = ConfigValue(
    "modes.outside_option.constant",
    key="constant",
    # default=0.0,
    expected_type=float,
    description="Constant utility of the outside option (€).",
)

ALPHA = ConfigValue(
    "modes.outside_option.alpha",
    key="alpha",
    default=0.0,
    expected_type=float,
    description="Value of time for the outside option (€/h).",
    note="This is usually not relevant as the outside option does not imply traveling.",
)

OUTSIDE_OPTION_ROAD_NETWORK_SPEED = ConfigValue(
    "modes.outside_option.road_network_speed",
    key="road_network_speed",
    expected_type=float,
    description="Constant speed on the road network to compute travel time for outside option trips (km/h).",
)

OUTSIDE_OPTION_TABLE = ConfigTable(
    "modes.outside_option",
    "outside_option",
    items=[CONSTANT, ALPHA, OUTSIDE_OPTION_ROAD_NETWORK_SPEED],
)


@error_context(msg="Cannot generate outside option preferences.")
def generate_outside_option_parameters(config: Config):
    trips = TRIPS_FILE.read(config)
    df = (
        trips.select("tour_id", outside_option_cst=pl.lit(config[CONSTANT], dtype=pl.Float64))
        .unique()
        .sort("tour_id")
    )
    tts = OUTSIDE_OPTION_TRAVEL_TIMES_FILE.read(config)
    if tts is not None:
        alpha = config[ALPHA]
        df = (
            df.join(tts, on="tour_id", how="left")
            .with_columns(
                outside_option_cst=pl.col("outside_option_cst")
                - alpha * pl.col("outside_option_travel_time").dt.total_seconds() / 3600.0
            )
            .drop("outside_option_travel_time")
        )
    OUTSIDE_OPTION_PARAMETERS_FILE.save(df, config)
    return True


@error_context(msg="Cannot generate outside-option travel times from road distances.")
def generate_outside_option_travel_times_from_road_distances(config: Config) -> bool:
    df = CAR_DRIVER_DISTANCES_FILE.read(config)
    speed = config[OUTSIDE_OPTION_ROAD_NETWORK_SPEED]
    df = df.select(
        "trip_id", outside_option_travel_time=pl.duration(seconds=pl.col("distance") / speed * 3.6)
    )
    trips = TRIPS_FILE.read(config)
    df = df.join(trips, on="trip_id", how="left")
    df = df.group_by("tour_id").agg(pl.col("outside_option_travel_time").sum())
    OUTSIDE_OPTION_TRAVEL_TIMES_FILE.save(df, config)
    return True


OUTSIDE_OPTION_PREFERENCES_STEP = Step(
    "outside-option-preferences",
    generate_outside_option_parameters,
    required_files=[TRIPS_FILE],
    optional_files=[OUTSIDE_OPTION_TRAVEL_TIMES_FILE],
    output_files=[OUTSIDE_OPTION_PARAMETERS_FILE],
    config_values=[CONSTANT, ALPHA],
)

OUTSIDE_OPTION_TRAVEL_TIMES_FROM_ROAD_DISTANCES_STEP = Step(
    "outside-option-travel-times-from-road-distances",
    generate_outside_option_travel_times_from_road_distances,
    required_files=[CAR_DRIVER_DISTANCES_FILE, TRIPS_FILE],
    output_files=[OUTSIDE_OPTION_TRAVEL_TIMES_FILE],
    config_values=[OUTSIDE_OPTION_ROAD_NETWORK_SPEED],
)
