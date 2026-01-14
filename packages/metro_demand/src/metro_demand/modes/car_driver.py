import polars as pl
from metro_common.errors import error_context
from metro_network.road_network.files import ALL_DISTANCES_FILE
from metro_pipeline import Config, ConfigTable, ConfigValue, Step

from metro_demand.population import PERSONS_FILE

from .files import CAR_DRIVER_DISTANCES_FILE, CAR_DRIVER_ODS_FILE, CAR_DRIVER_PARAMETERS_FILE

VALUE_OF_TIME = ConfigValue(
    "modes.car_driver.alpha",
    key="alpha",
    default=0.0,
    expected_type=float,
    description="Value of time as a car driver (€/h).",
)

CONSTANT = ConfigValue(
    "modes.car_driver.constant",
    key="constant",
    default=0.0,
    expected_type=float,
    description="Constant penalty for each trip as a car driver (€).",
)

CAR_DRIVER_TABLE = ConfigTable("modes.car_driver", "car_driver", items=[VALUE_OF_TIME, CONSTANT])


@error_context(msg="Cannot generate car driver preferences.")
def generate_car_driver_parameters(config: Config):
    persons = PERSONS_FILE.read(config)
    df = persons.select(
        "person_id",
        car_driver_cst=pl.lit(config[CONSTANT]),
        car_driver_vot=pl.lit(config[VALUE_OF_TIME]),
    )
    CAR_DRIVER_PARAMETERS_FILE.save(df, config)
    return True


CAR_DRIVER_PREFERENCES_STEP = Step(
    "car-driver-preferences",
    generate_car_driver_parameters,
    required_files=[PERSONS_FILE],
    output_files=[CAR_DRIVER_PARAMETERS_FILE],
    config_values=[VALUE_OF_TIME, CONSTANT],
)


@error_context(msg="Cannot compute car driver shortest-path distances.")
def compute_car_driver_distances(config: Config):
    trips = CAR_DRIVER_ODS_FILE.read(config)
    dists = ALL_DISTANCES_FILE.read(config)
    trips = trips.join(
        dists,
        left_on=["origin_node_id", "destination_node_id"],
        right_on=["origin_id", "destination_id"],
        how="left",
    )
    trips = trips.select("trip_id", "distance")
    CAR_DRIVER_DISTANCES_FILE.save(trips, config)
    return True


CAR_DRIVER_DISTANCES_STEP = Step(
    "car-driver-distances",
    compute_car_driver_distances,
    required_files=[CAR_DRIVER_ODS_FILE, ALL_DISTANCES_FILE],
    output_files=[CAR_DRIVER_DISTANCES_FILE],
)
