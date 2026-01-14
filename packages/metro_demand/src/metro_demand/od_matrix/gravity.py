import polars as pl
from metro_common.errors import error_context
from metro_network.road_network import ALL_FREE_FLOW_TRAVEL_TIMES_FILE
from metro_pipeline import RANDOM_SEED, Config, ConfigTable, ConfigValue, Step

from metro_demand.modes import CAR_DRIVER_ODS_FILE

from .common import generate_trips_from_od_matrix

EXPONENTIAL_DECAY = ConfigValue(
    "gravity_od_matrix.exponential_decay",
    key="exponential_decay",
    expected_type=float,
    description="Exponential decay rate of flows as a function of free-flow travel times (rate per minute)",
)

TRIPS_PER_NODE = ConfigValue(
    "gravity_od_matrix.trips_per_node",
    key="trips_per_node",
    expected_type=int,
    description="Number of trips to be generated originating from each node",
)

GRAVITY_OD_MATRIX_TABLE = ConfigTable(
    "gravity_od_matrix", "gravity_od_matrix", items=[EXPONENTIAL_DECAY, TRIPS_PER_NODE]
)


@error_context(msg="Cannot generate trips from a gravity model.")
def generate_trips_with_gravity_model(config: Config):
    df = ALL_FREE_FLOW_TRAVEL_TIMES_FILE.read(config)
    df = df.filter(pl.col("origin_id") != pl.col("destination_id"))
    decay = config[EXPONENTIAL_DECAY]
    df = df.with_columns(
        rate=(-pl.lit(decay) * pl.col("free_flow_travel_time").dt.total_seconds() / 60).exp()
    )
    df = df.with_columns(normalized_rate=pl.col("rate") / pl.col("rate").sum().over("origin_id"))
    df = df.with_columns(size=pl.col("normalized_rate") * config[TRIPS_PER_NODE])
    df = df.select(origin="origin_id", destination="destination_id", size="size")
    trips = generate_trips_from_od_matrix(df, config[RANDOM_SEED])
    CAR_DRIVER_ODS_FILE.save(trips, config)
    return True


GRAVITY_OD_MATRIX_STEP = Step(
    "gravity-od-matrix",
    generate_trips_with_gravity_model,
    required_files=[ALL_FREE_FLOW_TRAVEL_TIMES_FILE],
    output_files=[CAR_DRIVER_ODS_FILE],
    config_values=[EXPONENTIAL_DECAY],
)
