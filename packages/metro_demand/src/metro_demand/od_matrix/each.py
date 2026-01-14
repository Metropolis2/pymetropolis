import numpy as np
import polars as pl
from metro_common.errors import error_context
from metro_network.road_network import CLEAN_EDGES_FILE
from metro_pipeline import RANDOM_SEED, Config, ConfigTable, ConfigValue, Step

from metro_demand.modes import CAR_DRIVER_ODS_FILE

from .common import generate_trips_from_od_matrix

EACH = ConfigValue(
    "node_od_matrix.each",
    key="each",
    expected_type=float,
    description="Number of trips to generate for each origin-destination pair.",
)

NODE_OD_MATRIX_TABLE = ConfigTable("node_od_matrix", "node_od_matrix", items=[EACH])


@error_context(msg="Cannot generate a fixed number of trips for each origin-destination pair.")
def n_trips_for_each_od_pair(config: Config):
    edges = CLEAN_EDGES_FILE.read(config)
    sources = pl.Series(edges["source"]).unique().sort()
    targets = pl.Series(edges["target"]).unique().sort()
    each = config[EACH]
    df = pl.DataFrame(
        {
            "origin": np.repeat(sources, len(targets)),
            "destination": np.tile(targets, len(sources)),
            "size": each,
        }
    )
    trips = generate_trips_from_od_matrix(df, config[RANDOM_SEED])
    CAR_DRIVER_ODS_FILE.save(trips, config)
    return True


OD_MATRIX_EACH = Step(
    "od-matrix-each",
    n_trips_for_each_od_pair,
    required_files=[CLEAN_EDGES_FILE],
    output_files=[CAR_DRIVER_ODS_FILE],
    config_values=[EACH],
)
