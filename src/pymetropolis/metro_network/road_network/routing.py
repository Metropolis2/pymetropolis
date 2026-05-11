from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_common.utils import pl_duration_to_seconds
from pymetropolis.metro_pipeline import Step

from .files import (
    AllRoadDistancesFile,
    AllRoadFreeFlowTravelTimesFile,
    RoadEdgesCleanFile,
    RoadEdgesFreeFlowTravelTimeFile,
)

if TYPE_CHECKING:
    import polars as pl


def compute_all_pairs_dijkstra(edges: pl.DataFrame) -> pl.DataFrame:
    import networkx as nx
    import polars as pl

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


class AllFreeFlowTravelTimesStep(Step):
    """Computes travel time of the fastest path under (car) free-flow conditions, for all node pairs
    of the road network.
    """

    input_files = {"edges": RoadEdgesCleanFile, "edges_fftt": RoadEdgesFreeFlowTravelTimeFile}
    output_files = {"all_free_flow_travel_times": AllRoadFreeFlowTravelTimesFile}
    priority = 0

    def run(self):
        import polars as pl

        edges_gdf = self.input["edges"].read()
        edges = pl.from_pandas(edges_gdf.loc[:, ["edge_id", "source", "target"]])
        edges_fftt = self.input["edges_fftt"].read()
        edges = edges.join(edges_fftt, on="edge_id").select(
            "source", "target", weight=pl_duration_to_seconds("free_flow_travel_time")
        )
        df = compute_all_pairs_dijkstra(edges)
        df = df.with_columns(free_flow_travel_time=pl.duration(seconds="weight")).drop("weight")
        self.output["all_free_flow_travel_times"].write(df)


class AllRoadDistancesStep(Step):
    """Computes distance of the shortest path, for all node pairs of the road network."""

    input_files = {"clean_edges": RoadEdgesCleanFile}
    output_files = {"all_distances": AllRoadDistancesFile}
    priority = 0

    def run(self):
        import polars as pl

        edges = self.input["clean_edges"].read()
        edges = pl.from_pandas(edges.loc[:, ["edge_id", "source", "target", "length"]])
        edges = edges.select("source", "target", weight="length")
        df = compute_all_pairs_dijkstra(edges)
        df = df.rename({"weight": "distance"})
        self.output["all_distances"].write(df)
