import networkx as nx
import polars as pl

from pymetropolis.metro_pipeline import Step

from .files import (
    AllFreeFlowTravelTimesFile,
    AllRoadDistancesFile,
    CleanEdgesFile,
    EdgesFreeFlowTravelTimeFile,
)


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


class AllFreeFlowTravelTimesStep(Step):
    """Computes travel time of the fastest path under (car) free-flow conditions, for all node pairs
    of the road network.
    """

    input_files = {"edges": CleanEdgesFile, "edges_fftt": EdgesFreeFlowTravelTimeFile}
    output_files = {"all_free_flow_travel_times": AllFreeFlowTravelTimesFile}

    def run(self):
        edges_gdf = self.input["edges"].read()
        edges = pl.from_pandas(edges_gdf.loc[:, ["edge_id", "source", "target"]])
        edges_fftt = self.input["edges_fftt"].read()
        edges = edges.join(edges_fftt, on="edge_id").select(
            "source", "target", weight=pl.col("free_flow_travel_time").dt.total_seconds()
        )
        df = compute_all_pairs_dijkstra(edges)
        df = df.with_columns(free_flow_travel_time=pl.duration(seconds="weight")).drop("weight")
        self.output["all_free_flow_travel_times"].write(df)


class AllRoadDistancesStep(Step):
    """Computes distance of the shortest path, for all node pairs of the road network."""

    input_files = {"clean_edges": CleanEdgesFile}
    output_files = {"all_distances": AllRoadDistancesFile}

    def run(self):
        edges = self.input["clean_edges"].read()
        edges = pl.from_pandas(edges.loc[:, ["edge_id", "source", "target", "length"]])
        edges = edges.select("source", "target", weight="length")
        df = compute_all_pairs_dijkstra(edges)
        df = df.rename({"weight": "distance"})
        self.output["all_distances"].write(df)
