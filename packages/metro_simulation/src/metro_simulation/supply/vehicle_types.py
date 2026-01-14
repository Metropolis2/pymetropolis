import polars as pl
from metro_common.errors import error_context
from metro_pipeline import Config, ConfigTable, ConfigValue, Step

from .files import METRO_VEHICLE_TYPES_FILE

CAR_HEADWAY = ConfigValue(
    "vehicle_types.car.headway",
    key="headway",
    expected_type=float,
    description="Typical length between two cars, from head to head, in meters",
)

CAR_PCE = ConfigValue(
    "vehicle_types.car.pce",
    key="pce",
    expected_type=float,
    default=1.0,
    description="Passenger car equivalent of a typical car",
)

CAR_TABLE = ConfigTable(
    "vehicles_types.car",
    "car",
    items=[CAR_HEADWAY, CAR_PCE],
)

VEHICLE_TYPES_TABLE = ConfigTable(
    "vehicle_types",
    "vehicle_types",
    items=[CAR_TABLE],
)


@error_context(msg="Cannot write metro vehicle types file")
def write_vehicle_types(config: Config):
    df = pl.DataFrame(
        {"vehicle_id": ["car_driver"], "headway": [config[CAR_HEADWAY]], "pce": [config[CAR_PCE]]}
    )
    METRO_VEHICLE_TYPES_FILE.save(df, config)
    return True


WRITE_VEHICLE_TYPES_STEP = Step(
    "write-vehicle-types",
    write_vehicle_types,
    output_files=[METRO_VEHICLE_TYPES_FILE],
    config_values=[CAR_HEADWAY, CAR_PCE],
)
