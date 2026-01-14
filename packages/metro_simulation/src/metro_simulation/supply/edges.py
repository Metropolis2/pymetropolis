import polars as pl
from metro_common.errors import error_context
from metro_common.utils import time_to_seconds_since_midnight_pl
from metro_network.road_network import CLEAN_EDGES_FILE, EDGES_CAPACITIES_FILE, EDGES_PENALTIES_FILE
from metro_pipeline import Config, Step

from .files import METRO_EDGES_FILE


@error_context(msg="Cannot write metro edges file")
def write_edges(config: Config):
    edges = CLEAN_EDGES_FILE.read(config)
    columns = ["edge_id", "source", "target", "length", "speed_limit", "lanes"]
    df = pl.from_pandas(edges.loc[:, columns])
    df = df.select(
        "edge_id",
        "source",
        "target",
        "length",
        "lanes",
        speed=pl.col("speed_limit") / 3.6,
        overtaking=pl.lit(True),
    )
    capacities = EDGES_CAPACITIES_FILE.read_if_exists(config)
    if capacities is not None:
        df = (
            df.join(capacities, on="edge_id", how="left")
            .with_columns(
                bottleneck_flow=pl.col("capacity") / 3600.0,
                bottleneck_flows=pl.col("capacities").list.eval(pl.element() / 3600.0),
                bottleneck_times=pl.col("times").list.eval(
                    time_to_seconds_since_midnight_pl(pl.element())
                ),
            )
            .drop("capacity", "capacities", "times")
        )
        if df["bottleneck_flows"].is_null().all():
            df = df.drop("bottleneck_flows", "bottleneck_times")
    penalties = EDGES_PENALTIES_FILE.read_if_exists(config)
    if penalties is not None:
        df = df.join(penalties, on="edge_id", how="left").rename(
            {"constant": "constant_travel_time"}
        )
    METRO_EDGES_FILE.save(df, config)
    return True


WRITE_EDGES_STEP = Step(
    "write-edges",
    write_edges,
    required_files=[CLEAN_EDGES_FILE],
    optional_files=[EDGES_CAPACITIES_FILE, EDGES_PENALTIES_FILE],
    output_files=[METRO_EDGES_FILE],
)
