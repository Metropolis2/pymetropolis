import polars as pl

from pymetropolis.metro_common.errors import error_context
from pymetropolis.metro_demand.population import PERSONS_FILE
from pymetropolis.metro_pipeline import Config, ConfigTable, ConfigValue, Step

from .files import (
    CAR_DRIVER_DISTANCES_FILE,
    PUBLIC_TRANSIT_PARAMETERS_FILE,
    PUBLIC_TRANSIT_TRAVEL_TIMES_FILE,
)

VALUE_OF_TIME = ConfigValue(
    "modes.public_transit.alpha",
    key="alpha",
    default=0.0,
    expected_type=float,
    description="Value of time in public transit (€/h).",
)

CONSTANT = ConfigValue(
    "modes.public_transit.constant",
    key="constant",
    default=0.0,
    expected_type=float,
    description="Constant penalty for each trip in public transit (€).",
)

PUBLIC_TRANSIT_ROAD_NETWORK_SPEED = ConfigValue(
    "modes.public_transit.road_network_speed",
    key="road_network_speed",
    expected_type=float,
    description="Speed of public-transit vehicles on the road network (km/h).",
)

PUBLIC_TRANSIT_TABLE = ConfigTable(
    "modes.public_transit",
    "public_transit",
    items=[VALUE_OF_TIME, CONSTANT, PUBLIC_TRANSIT_ROAD_NETWORK_SPEED],
)


@error_context(msg="Cannot generate public-transit preferences.")
def generate_public_transit_parameters(config: Config):
    persons = PERSONS_FILE.read(config)
    df = persons.select(
        "person_id",
        public_transit_cst=pl.lit(config[CONSTANT]),
        public_transit_vot=pl.lit(config[VALUE_OF_TIME]),
    )
    PUBLIC_TRANSIT_PARAMETERS_FILE.save(df, config)
    return True


PUBLIC_TRANSIT_PREFERENCES_STEP = Step(
    "public-transit-preferences",
    generate_public_transit_parameters,
    required_files=[PERSONS_FILE],
    output_files=[PUBLIC_TRANSIT_PARAMETERS_FILE],
    config_values=[VALUE_OF_TIME, CONSTANT],
)


@error_context(msg="Cannot generate public-transit travel times from road distances.")
def generate_public_transit_travel_times_from_road_distances(config: Config) -> bool:
    df = CAR_DRIVER_DISTANCES_FILE.read(config)
    speed = config[PUBLIC_TRANSIT_ROAD_NETWORK_SPEED]
    df = df.select(
        "trip_id", public_transit_travel_time=pl.duration(seconds=pl.col("distance") / speed * 3.6)
    )
    PUBLIC_TRANSIT_TRAVEL_TIMES_FILE.save(df, config)
    return True


PUBLIC_TRANSIT_TRAVEL_TIMES_FROM_ROAD_DISTANCES_STEP = Step(
    "public-transit-travel-times-from-road-distances",
    generate_public_transit_travel_times_from_road_distances,
    required_files=[CAR_DRIVER_DISTANCES_FILE],
    output_files=[PUBLIC_TRANSIT_TRAVEL_TIMES_FILE],
    config_values=[PUBLIC_TRANSIT_ROAD_NETWORK_SPEED],
)
