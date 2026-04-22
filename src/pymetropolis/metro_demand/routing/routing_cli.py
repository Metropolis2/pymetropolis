import json
import os
import subprocess
import tempfile
from pathlib import Path

import polars as pl

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_demand.routing.files import (
    TripsPedestrianDistancesFile,
    TripsPedestrianNodesFile,
)
from pymetropolis.metro_network.pedestrian_network.files import PedestrianEdgesCleanFile
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import BoolParameter, ExecPathParameter


class RoutingCLIStep(Step):
    """Abstract Step to run the Metropolis-Core routing executable."""

    exec_path = ExecPathParameter(
        "metropolis_core.routing_exec_path",
        description="Path to the `routing_cli` executable.",
        note='On Windows, you can omit the ".exe" extension',
    )

    def is_defined(self) -> bool:
        return self.exec_path is not None


class TripsPedestrianDistancesStep(RoutingCLIStep):
    """Computes the trips' distance on the pedestrian network.

    The distance is defined as the length of the shortest path from origin to destination node on
    the pedestrian network.

    If the [`output_path`](parameters.md#pedestrian_routingoutput_path) is set to `true`, the list
    of pedestrian edges along the shortest path is stored in the `pedestrian_path` column of the
    [`TripsPedestrianDistancesFile`](files.md#tripspedestriandistancesfile) file.
    """

    output_path = BoolParameter(
        "pedestrian_routing.output_path",
        default=False,
        description="Whether the shortest paths are stored.",
    )
    input_files = {"od_pairs": TripsPedestrianNodesFile, "edges": PedestrianEdgesCleanFile}
    output_files = {"distances": TripsPedestrianDistancesFile}

    def run(self):
        edges_gdf = self.input["edges"].read()
        edges = pl.from_pandas(edges_gdf.loc[:, ["edge_id", "source", "target", "length"]]).rename(
            {"length": "weight"}
        )
        od_pairs = self.input["od_pairs"].read()
        trips = od_pairs.select(
            "trip_id",
            origin_node="origin_pedestrian_node",
            destination_node="destination_pedestrian_node",
        )
        df = trip_routing(trips, edges, self.exec_path, self.output_path)
        if self.output_path:
            df = df.select("trip_id", pedestrian_distance="value", pedestrian_path="route")
            breakpoint()
        else:
            df = df.select("trip_id", pedestrian_distance="value")
        self.output["distances"].write(df)


def trip_routing(
    trips: pl.DataFrame, edges: pl.DataFrame, routing_exec: Path, with_routes: bool = False
):
    queries = trips.select(query_id="trip_id", origin="origin_node", destination="destination_node")
    with tempfile.TemporaryDirectory() as tmp_directory:
        prepare_routing(queries, edges, tmp_directory, with_routes)
        run_routing(routing_exec, tmp_directory)
        df = pl.read_parquet(os.path.join(tmp_directory, "output", "ea_results.parquet"))
        breakpoint()
    if with_routes:
        df = df.select(trip_id="query_id", value="arrival_time", route="route")
    else:
        df = df.select(trip_id="query_id", value="arrival_time")
    return df


def prepare_routing(
    queries: pl.DataFrame, edges: pl.DataFrame, tmp_directory: str, with_routes: bool = False
):
    queries = queries.select("query_id", "origin", "destination", departure_time=pl.lit(0.0))
    queries.write_parquet(os.path.join(tmp_directory, "queries.parquet"))
    edges = edges.select("edge_id", "source", "target", "weight")
    # Parallel edges are removed, keeping only the minimum-weight edge.
    edges = edges.sort("weight").unique(subset=["source", "target"], keep="first").sort("edge_id")
    edges.rename({"weight": "travel_time"}).write_parquet(
        os.path.join(tmp_directory, "edges.parquet")
    )
    parameters = {
        "algorithm": "TCH" if with_routes else "Best",
        "output_route": with_routes,
        "input_files": {"queries": "queries.parquet", "edges": "edges.parquet"},
        "output_directory": "output",
        "saving_format": "Parquet",
    }
    with open(os.path.join(tmp_directory, "parameters.json"), "w") as f:
        json.dump(parameters, f)


def run_routing(routing_exec: Path, tmp_directory: str):
    parameters_filename = os.path.join(tmp_directory, "parameters.json")
    res = subprocess.run([routing_exec, parameters_filename], check=False)
    if res.returncode:
        # The run did not succeed.
        raise MetropyError("Metropolis-Core routing failed.")
