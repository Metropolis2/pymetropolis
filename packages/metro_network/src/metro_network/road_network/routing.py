import networkx as nx
import polars as pl
from metro_pipeline import Config, Step

from .files import (
    ALL_DISTANCES_FILE,
    ALL_FREE_FLOW_TRAVEL_TIMES_FILE,
    CLEAN_EDGES_FILE,
    EDGES_PENALTIES_FILE,
)


def compute_free_flow_travel_times(config: Config) -> bool:
    edges = CLEAN_EDGES_FILE.read(config)
    edges = pl.from_pandas(edges.loc[:, ["edge_id", "source", "target", "length", "speed_limit"]])
    penalties = EDGES_PENALTIES_FILE.read_if_exists(config)
    if penalties is None:
        edges = edges.with_columns(constant=pl.lit(0.0, dtype=pl.Float64))
    else:
        edges = edges.join(penalties, on="edge_id", how="left")
    edges = edges.select(
        "source", "target", tt=pl.col("length") / pl.col("speed_limit") * 3.6 + pl.col("constant")
    )
    df = compute_all_pairs_dijkstra(edges)
    df = df.with_columns(free_flow_travel_time=pl.duration(seconds="weight")).drop("weight")
    ALL_FREE_FLOW_TRAVEL_TIMES_FILE.save(df, config)
    return True


def compute_shortest_path_distances(config: Config) -> bool:
    edges = CLEAN_EDGES_FILE.read(config)
    edges = pl.from_pandas(edges.loc[:, ["edge_id", "source", "target", "length"]])
    edges = edges.select("source", "target", weight="length")
    df = compute_all_pairs_dijkstra(edges)
    df = df.rename({"weight": "distance"})
    ALL_DISTANCES_FILE.save(df, config)
    return True


def compute_all_pairs_dijkstra(edges: pl.DataFrame) -> pl.DataFrame:
    dtype = edges["source"].dtype
    G = nx.DiGraph()
    G.add_weighted_edges_from(edges.iter_rows(), weight="weight")
    ods = list()
    for origin, data in nx.all_pairs_dijkstra_path_length(G, weight="weight"):
        for destination, weight in data.items():
            ods.append((origin, destination, weight))
    df = pl.DataFrame(
        ods,
        orient="row",
        schema={"origin_id": dtype, "destination_id": dtype, "weight": pl.Float64},
    )
    return df


ALL_FREE_FLOW_TRAVEL_TIMES_STEP = Step(
    "all-free-flow-travel-times",
    compute_free_flow_travel_times,
    required_files=[CLEAN_EDGES_FILE],
    optional_files=[EDGES_PENALTIES_FILE],
    output_files=[ALL_FREE_FLOW_TRAVEL_TIMES_FILE],
)

ALL_DISTANCES_STEP = Step(
    "all-distances",
    compute_shortest_path_distances,
    required_files=[CLEAN_EDGES_FILE],
    output_files=[ALL_DISTANCES_FILE],
)
