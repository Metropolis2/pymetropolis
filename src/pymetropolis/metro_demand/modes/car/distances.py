import itertools

import networkx as nx
import polars as pl

from pymetropolis.metro_network.road_network import AllRoadDistancesFile
from pymetropolis.metro_network.road_network.files import (
    RoadEdgesCleanFile,
    RoadEdgesFreeFlowTravelTimeFile,
)
from pymetropolis.metro_pipeline import Step

from .files import CarFreeFlowDistancesFile, CarODsFile, CarShortestDistancesFile


class CarShortestDistancesStep(Step):
    """Generates the distance of the shortest path on the road network for each trip, given the
    origin and destination as a car driver.

    The shortest-path distances are not computed but are read from the file containing the
    shortest-path distances of all node pairs (AllRoadDistancesFile).
    """

    input_files = {"car_driver_ods": CarODsFile, "all_distances": AllRoadDistancesFile}
    output_files = {"car_distances": CarShortestDistancesFile}

    def run(self):
        trips: pl.DataFrame = self.input["car_driver_ods"].read()
        dists: pl.DataFrame = self.input["all_distances"].read()
        trips = trips.join(
            dists,
            left_on=["origin_node_id", "destination_node_id"],
            right_on=["origin_id", "destination_id"],
            how="left",
        )
        trips = trips.select("trip_id", "distance")
        self.output["car_distances"].write(trips)


# TODO. How to consider road restrictions?
class CarFreeFlowDistancesStep(Step):
    """Generates the distance of the shortest path on the road network for each trip, given the
    origin and destination as a car driver.

    The shortest-path distances are not computed but are read from the file containing the
    shortest-path distances of all node pairs (AllRoadDistancesFile).
    """

    input_files = {
        "car_driver_ods": CarODsFile,
        "edges": RoadEdgesCleanFile,
        "edges_fftt": RoadEdgesFreeFlowTravelTimeFile,
    }
    output_files = {"car_distances": CarFreeFlowDistancesFile}

    def run(self):
        edges_gdf = self.input["edges"].read()
        edges = pl.from_pandas(edges_gdf.loc[:, ["edge_id", "source", "target", "length"]])
        edges_fftt = self.input["edges_fftt"].read()
        edges = edges.join(edges_fftt, on="edge_id").with_columns(
            fftt=pl.col("free_flow_travel_time").dt.total_seconds()
        )
        edges = edges.select("source", "target", "length", "fftt")
        ods = self.input["car_driver_ods"].read()
        distances = compute_distances(edges, ods)
        df = ods.join(distances, on=["origin_node_id", "destination_node_id"], how="left").select(
            "trip_id", "distance"
        )
        self.output["car_distances"].write(df)


def compute_distances(edges: pl.DataFrame, ods: pl.DataFrame) -> pl.DataFrame:
    dtype = edges["source"].dtype
    G = nx.DiGraph()
    G.add_edges_from(
        map(lambda row: (row.pop("source"), row.pop("target"), row), edges.iter_rows(named=True))
    )
    results = list()
    for row in (
        ods.group_by("origin_node_id")
        .agg(pl.col("destination_node_id").unique())
        .iter_rows(named=True)
    ):
        origin = row["origin_node_id"]
        destinations = set(row["destination_node_id"])
        for destination, path in nx.single_source_dijkstra_path(G, origin, weight="fftt").items():
            if destination in destinations:
                distance = sum(G[u][v]["length"] for u, v in itertools.pairwise(path))
                results.append((origin, destination, distance))
    df = pl.DataFrame(
        results,
        orient="row",
        schema={"origin_node_id": dtype, "destination_node_id": dtype, "distance": pl.Float64},
    )
    return df


def get_path_distance(path: list, edges_length: dict) -> float:
    return sum(edges_length[e] for e in path)
