from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_calibration.road.files import RoadEdgesFreeFlowTravelTimeFile
from pymetropolis.metro_demand.routing.files import (
    ZonesLevel1RoadNodeFile,
    ZonesLevel2RoadNodeFile,
    ZonesLevel3RoadNodeFile,
    ZonesLevel4RoadNodeFile,
    ZonesLevel5RoadNodeFile,
)
from pymetropolis.metro_demand.routing.routing_cli import RoutingCLIStep, run_routing, trip_routing
from pymetropolis.metro_network.road_network.files import RoadEdgesCleanFile
from pymetropolis.metro_pipeline.parameters import ListParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_pipeline.types import Int, Time
from pymetropolis.metro_simulation.run.files import MetroNextExpectedTravelTimeFunctionsFile

from .files import (
    ZoneODLevel1CongestedTravelTimesFile,
    ZoneODLevel1FreeFlowTravelTimesFile,
    ZoneODLevel2CongestedTravelTimesFile,
    ZoneODLevel2FreeFlowTravelTimesFile,
    ZoneODLevel3CongestedTravelTimesFile,
    ZoneODLevel3FreeFlowTravelTimesFile,
    ZoneODLevel4CongestedTravelTimesFile,
    ZoneODLevel4FreeFlowTravelTimesFile,
    ZoneODLevel5CongestedTravelTimesFile,
    ZoneODLevel5FreeFlowTravelTimesFile,
)

if TYPE_CHECKING:
    import polars as pl


class ZonesODFreeFlowTravelTimesStep(RoutingCLIStep):
    """Computes the free-flow travel time by car between each ordered pair of zones, using each
    zone's representative road node (ZonesRoadNodeFile) as a virtual origin/destination.
    """

    zones = ListParameter(
        "od_matrix_travel_times.free_flow_zones_levels",
        inner=Int(lb=1, ub=5),
        max_length=5,
        description="Zones levels for which OD free-flow travel times are computed.",
        default=None,
        example="`[3, 4]`",
    )
    input_files = {
        "zone1_road_node": InputFile(
            ZonesLevel1RoadNodeFile, when=lambda step: step.zones is not None and 1 in step.zones
        ),
        "zone2_road_node": InputFile(
            ZonesLevel2RoadNodeFile, when=lambda step: step.zones is not None and 2 in step.zones
        ),
        "zone3_road_node": InputFile(
            ZonesLevel3RoadNodeFile, when=lambda step: step.zones is not None and 3 in step.zones
        ),
        "zone4_road_node": InputFile(
            ZonesLevel4RoadNodeFile, when=lambda step: step.zones is not None and 4 in step.zones
        ),
        "zone5_road_node": InputFile(
            ZonesLevel5RoadNodeFile, when=lambda step: step.zones is not None and 5 in step.zones
        ),
        "edges": RoadEdgesCleanFile,
        "edges_fftt": RoadEdgesFreeFlowTravelTimeFile,
    }
    output_files = {
        "zone1_fftt": ZoneODLevel1FreeFlowTravelTimesFile,
        "zone2_fftt": ZoneODLevel2FreeFlowTravelTimesFile,
        "zone3_fftt": ZoneODLevel3FreeFlowTravelTimesFile,
        "zone4_fftt": ZoneODLevel4FreeFlowTravelTimesFile,
        "zone5_fftt": ZoneODLevel5FreeFlowTravelTimesFile,
    }

    def is_defined(self) -> bool:
        return super().is_defined() and self.zones is not None

    def run(self):
        import polars as pl

        assert self.exec_path is not None
        assert self.zones is not None

        edges_gdf = self.input["edges"].read()
        edges_fftt = self.input["edges_fftt"].read()
        edges = (
            pl.from_pandas(edges_gdf.loc[:, ["edge_id", "source", "target", "length"]])
            .join(edges_fftt, on="edge_id", how="left")
            .with_columns(pl.col("free_flow_travel_time").dt.total_nanoseconds() / 1e9)
            .rename({"free_flow_travel_time": "weight"})
        )
        n = edges["weight"].null_count()
        if n:
            logger.warning(f"Discarding {n} edges with NULL free-flow travel time.")
            edges = edges.filter(pl.col("weight").is_not_null())

        for zone in self.zones:
            zones_df = self.input[f"zone{zone}_road_node"].read().select("zone_id", "road_node")
            pairs, trips = read_trips(zones_df)
            results = trip_routing(trips, edges, self.exec_path, with_routes=False)
            results = results.select(
                query_id="trip_id", free_flow_travel_time=pl.duration(seconds="value")
            )
            df = pairs.join(results, on="query_id", how="left", coalesce=False).select(
                "origin_zone_id", "destination_zone_id", "free_flow_travel_time"
            )
            self.output[f"zone{zone}_fftt"].write(df)


class ZonesODCongestedTravelTimesStep(RoutingCLIStep):
    """Computes the congested travel time by car between each ordered pair of zones, using each
    zone's representative road node as a virtual origin/destination.

    Unlike the free-flow variant, results depend on departure time: routing is
    run once per zone pair againt the congested edge travel-time functions
    produced by the simulation (MetroNextExpectedTravlTimeFunctionsFile),
    giving one breakpoint every recording_interval. The stored value is the median
    travel time over breakpoints falling within `time_window` (along with the min,
    max and standard deviation over the same breakpoints).
    If `time_window` is not specified, the full time window of the simulation is used.
    """

    time_window = ListParameter(
        "od_matrix_travel_times.time_window",
        inner=Time(),
        length=2,
        description="Time window over which the congested travel time is aggregated.",
        example="`[06:00:00, 09:00:00]`",
    )
    zones = ListParameter(
        "od_matrix_travel_times.congested_zones_levels",
        inner=Int(),
        max_length=5,
        description="Zones levels for which OD congested travel times are computed.",
        default=None,
        example="`[3, 4]`",
    )

    input_files = {
        "zone1_road_node": InputFile(
            ZonesLevel1RoadNodeFile, when=lambda step: step.zones is not None and 1 in step.zones
        ),
        "zone2_road_node": InputFile(
            ZonesLevel2RoadNodeFile, when=lambda step: step.zones is not None and 2 in step.zones
        ),
        "zone3_road_node": InputFile(
            ZonesLevel3RoadNodeFile, when=lambda step: step.zones is not None and 3 in step.zones
        ),
        "zone4_road_node": InputFile(
            ZonesLevel4RoadNodeFile, when=lambda step: step.zones is not None and 4 in step.zones
        ),
        "zone5_road_node": InputFile(
            ZonesLevel5RoadNodeFile, when=lambda step: step.zones is not None and 5 in step.zones
        ),
        "edges": RoadEdgesCleanFile,
        "edges_fftt": RoadEdgesFreeFlowTravelTimeFile,
        "edge_ttfs": MetroNextExpectedTravelTimeFunctionsFile,
    }
    output_files = {
        "zone1_congested": ZoneODLevel1CongestedTravelTimesFile,
        "zone2_congested": ZoneODLevel2CongestedTravelTimesFile,
        "zone3_congested": ZoneODLevel3CongestedTravelTimesFile,
        "zone4_congested": ZoneODLevel4CongestedTravelTimesFile,
        "zone5_congested": ZoneODLevel5CongestedTravelTimesFile,
    }

    def is_defined(self) -> bool:
        return super().is_defined() and self.zones is not None

    def run(self):
        import polars as pl

        assert self.exec_path is not None
        assert self.zones is not None

        edges_gdf = self.input["edges"].read()
        edges_fftt = self.input["edges_fftt"].read()
        edges = (
            pl.from_pandas(edges_gdf.loc[:, ["edge_id", "source", "target", "length"]])
            .join(edges_fftt, on="edge_id", how="left")
            .with_columns(pl.col("free_flow_travel_time").dt.total_nanoseconds() / 1e9)
            .rename({"free_flow_travel_time": "weight"})
        )
        n = edges["weight"].null_count()
        if n:
            logger.warning(f"Discarding {n} edges with NULL free-flow travel time.")
            edges = edges.filter(pl.col("weight").is_not_null())

        edge_ttfs = self.input["edge_ttfs"].read()
        if self.time_window is not None:
            # Remove breakpoints before the start of the time window (we don't need them and they
            # slow down the queries).
            edge_ttfs = edge_ttfs.filter(pl.col("departure_time") >= self.time_window[0].seconds())
        edge_ttfs = (
            edge_ttfs.filter(pl.col("vehicle_id") == "car_driver_alone")
            .select("edge_id", "departure_time", "travel_time")
            .join(edges.select("edge_id"), on="edge_id", how="semi")
        )

        for zone in self.zones:
            zones_df = self.input[f"zone{zone}_road_node"].read().select("zone_id", "road_node")
            pairs, trips = read_trips(zones_df)
            with tempfile.TemporaryDirectory() as tmp_directory:
                processing_routing(trips, edges, self.exec_path, edge_ttfs, tmp_directory)
                df = pl.read_parquet(
                    os.path.join(tmp_directory, "output", "profile_results.parquet")
                )
            if self.time_window is not None:
                # A null departure_time means the travel time is constant over the whole
                # simulated period (the route was never affected by congestion).
                df = df.filter(
                    pl.col("departure_time").is_null()
                    | (self.time_window[0].seconds() <= pl.col("departure_time"))
                    & (self.time_window[1].seconds() >= pl.col("departure_time"))
                )
            results = df.group_by("query_id").agg(
                congested_travel_time=pl.duration(seconds=pl.col("travel_time").median()),
                congested_travel_time_min=pl.duration(seconds=pl.col("travel_time").min()),
                congested_travel_time_max=pl.duration(seconds=pl.col("travel_time").max()),
                congested_travel_time_std=pl.duration(
                    seconds=pl.col("travel_time").std().fill_null(0.0)
                ),
            )
            df = pairs.join(results, on="query_id", how="left", coalesce=False).select(
                "origin_zone_id",
                "destination_zone_id",
                "congested_travel_time",
                "congested_travel_time_min",
                "congested_travel_time_max",
                "congested_travel_time_std",
            )
            self.output[f"zone{zone}_congested"].write(df)


def read_trips(zones: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Builds the zone x zone virtual "trips" used as OD queries for routing:
    every ordered pair of distinct zones, using each zone's representative road
    node as origin/destination.

    Returns two views of the same pairs: 'pairs' keeps the zone ids (used to join
    the routing results back to zone ids once routing is done),'trips' keeps only
    the node ids, renamed to the format expected by 'trip_routing()/processing_routing()'
    ('trip_id', 'origin_node', 'destination_node')
    """
    import polars as pl

    pairs = zones.select(origin_zone_id="zone_id", origin_node="road_node").join(
        zones.select(destination_zone_id="zone_id", destination_node="road_node"), how="cross"
    )
    pairs = pairs.filter(pl.col("origin_zone_id") != pl.col("destination_zone_id")).with_columns(
        query_id=pl.int_range(0, pl.len(), dtype=pl.UInt64)
    )
    trips = pairs.select("query_id", "origin_node", "destination_node").rename(
        {"query_id": "trip_id"}
    )
    return pairs, trips


def processing_routing(
    trips: pl.DataFrame,
    edges: pl.DataFrame,
    routing_exec_path: Path,
    edge_ttfs: pl.DataFrame,
    tmp_directory: str,
):
    """Runs the routing executable in "Intersect" mode (temporal profile) for a
    set of OD queries.

    This is the profile-mode equivalent of 'prepare_routing()'/'run_routing' in
    'routing_cli.py', which only supports a single fixed departure time per query
    (the executable returns the full travel time profile instead of a single value),
    and the edge-level congested travel-time functions ('edge_ttfs') are written
    alongside the statuc edge weights so the executable can reconstruct each edge's
    travel time at any departure_time. Results are read separately by the caller
    from 'tmp_directory/output/profile_results.parquet'.
    """
    import polars as pl

    queries = trips.select(
        query_id="trip_id",
        origin="origin_node",
        destination="destination_node",
        departure_time=pl.lit(None, dtype=pl.Float64),
    )
    queries.write_parquet(os.path.join(tmp_directory, "queries.parquet"))

    edges = edges.select("edge_id", "source", "target", "weight")
    edges = edges.sort("weight").unique(subset=["source", "target"], keep="first").sort("edge_id")
    edges.rename({"weight": "travel_time"}).write_parquet(
        os.path.join(tmp_directory, "edges.parquet")
    )

    edge_ttfs.write_parquet(os.path.join(tmp_directory, "edge_ttfs.parquet"))

    parameters = {
        "algorithm": "Intersect",
        "output_route": False,
        "input_files": {
            "queries": "queries.parquet",
            "edges": "edges.parquet",
            "edge_ttfs": "edge_ttfs.parquet",
        },
        "output_directory": "output",
        "saving_format": "Parquet",
    }
    with open(os.path.join(tmp_directory, "parameters.json"), "w") as f:
        json.dump(parameters, f)
    run_routing(routing_exec_path, tmp_directory)
