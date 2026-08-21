from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import geopandas as gpd
import polars as pl
from loguru import logger

from pymetropolis.metro_calibration.road.files import RoadEdgesFreeFlowTravelTimeFile
from pymetropolis.metro_demand.zones.file import (
    ZonesLevel1File,
    ZonesLevel2File,
    ZonesLevel3File,
    ZonesLevel4File,
    ZonesLevel5File,
)
from pymetropolis.metro_network.road_network.files import RoadEdgesCleanFile
from pymetropolis.metro_pipeline.parameters import ListParameter
from pymetropolis.metro_pipeline.types import String, Time, Int
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_simulation.run.files import MetroNextExpectedTravelTimeFunctionsFile
from pymetropolis.metro_spatial import GeoStep

from .files import (
    ZoneODLevel1CongestedTravelTimesFile,
    ZoneODLevel2CongestedTravelTimesFile,
    ZoneODLevel3CongestedTravelTimesFile,
    ZoneODLevel4CongestedTravelTimesFile,
    ZoneODLevel5CongestedTravelTimesFile,
    ZoneODLevel1FreeFlowTravelTimesFile,
    ZoneODLevel2FreeFlowTravelTimesFile,
    ZoneODLevel3FreeFlowTravelTimesFile,
    ZoneODLevel4FreeFlowTravelTimesFile,
    ZoneODLevel5FreeFlowTravelTimesFile,
    ZonesLevel1RoadNodeFile,
    ZonesLevel2RoadNodeFile,
    ZonesLevel3RoadNodeFile,
    ZonesLevel4RoadNodeFile,
    ZonesLevel5RoadNodeFile
)

from .routing_cli import RoutingCLIStep, run_routing, trip_routing


class ZonesBaseModel(GeoStep):
    forbidden_types = ListParameter(
        "road_network.forbiden_types",
        inner=String(),
        default=[],
        description=(
            "List of road edges' types that cannot be used as a zone's road"
            "node."
        ),
        example="['motorway', 'motorway_link', 'trunk', 'trunk_link']"
    )

    def find_origin_destination_node(
        self,
        zones: gpd.GeoDataFrame,
        edges: gpd.GeoDataFrame
    ) -> pl.DataFrame:
        import pandas as pd
        from scipy.spatial import distance_matrix
        from shapely.geometry import Point

        logger.debug("Listing candidate road-network nodes")
        source_nodes = edges[["source", "geometry"]].rename(columns={"source": "node"})
        source_nodes["geometry"] = source_nodes["geometry"].apply(lambda g: Point(g.coords[0]))
        target_nodes = edges[["target", "geometry"]].rename(columns={"target": "node"})
        target_nodes["geometry"] = target_nodes["geometry"].apply(lambda g: Point(g.coords[-1]))
        nodes = gpd.GeoDataFrame(
            pd.concat([source_nodes, target_nodes], ignore_index=True),
            crs=edges.crs
        ).drop_duplicates(subset="node")

        logger.debug("Assigning each road-network node to its zone")
        nodes = nodes.sjoin(zones, how="inner", predicate="within").drop(
            columns=["index_right"]
        )

        logger.debug("Finding the medoid node in each zone")
        nodes["x"] = nodes.geometry.x
        nodes["y"] = nodes.geometry.y
        df = pl.from_pandas(nodes.loc[:, ["node", "zone_id", "x", "y"]])
        medoids: dict = {}
        for (zone_id,), zone_df in df.partition_by(
            "zone_id", as_dict=True, include_key=False
        ).items():
            xy = zone_df.select("x", "y").to_numpy()
            dists = distance_matrix(xy, xy)
            medoid_idx = int(dists.sum(axis=1).argmin())
            medoids[zone_id] = zone_df["node"][medoid_idx]
        return pl.DataFrame(
            {
                "zone_id": list(medoids.keys()),
                "road_node": list(medoids.values())
            }
        ).with_columns(pl.col("road_node").cast(pl.UInt64))

    @property
    def input_zone(self):
        raise NotImplementedError

    @property
    def output_zone(self):
        raise NotImplementedError

    def run(self):
        zones = self.input_zone.read()
        zones = zones.to_crs(self.crs)
        edges = self.input["edges"].read()
        edges = edges.loc[
            ~edges["edge_type"].isin(self.forbidden_types),
            ["edge_id", "geometry", "source", "target"],
        ]
        nodes = self.find_origin_destination_node(zones, edges)
        self.output_zone.write(nodes)

class ZonesLevel1RoadNodesStep(ZonesBaseModel):
    input_files = {"zone1": ZonesLevel1File, "edges": RoadEdgesCleanFile}
    output_files = {"zone1_road_node": ZonesLevel1RoadNodeFile}

    @property
    def input_zone(self):
        return self.input["zone1"]

    @property
    def output_zone(self):
        return self.output["zone1_road_node"]

class ZonesLevel2RoadNodesStep(ZonesBaseModel):
    input_files = {"zone2": ZonesLevel2File, "edges": RoadEdgesCleanFile}
    output_files = {"zone2_road_node": ZonesLevel2RoadNodeFile}

    @property
    def input_zone(self):
        return self.input["zone2"]

    @property
    def output_zone(self):
        return self.output["zone2_road_node"]

class ZonesLevel3RoadNodesStep(ZonesBaseModel):
    input_files = {"zone3": ZonesLevel3File, "edges": RoadEdgesCleanFile}
    output_files = {"zone3_road_node": ZonesLevel3RoadNodeFile}

    @property
    def input_zone(self):
        return self.input["zone3"]

    @property
    def output_zone(self):
        return self.output["zone3_road_node"]

class ZonesLevel4RoadNodesStep(ZonesBaseModel):
    input_files = {"zone4": ZonesLevel4File, "edges": RoadEdgesCleanFile}
    output_files = {"zone4_road_node": ZonesLevel4RoadNodeFile}

    @property
    def input_zone(self):
        return self.input["zone4"]

    @property
    def output_zone(self):
        return self.output["zone4_road_node"]

class ZonesLevel5RoadNodesStep(ZonesBaseModel):
    input_files = {"zone5": ZonesLevel5File, "edges": RoadEdgesCleanFile}
    output_files = {"zone5_road_node": ZonesLevel5RoadNodeFile}

    @property
    def input_zone(self):
        return self.input["zone5"]

    @property
    def output_zone(self):
        return self.output["zone5_road_node"]


class ZonesODFreeFlowTravelTimesStep(RoutingCLIStep):
    """
    Computes the free-flow travel time by car between each ordered pair of
    zones, using each zone's representative road node (ZonesRoadNodeFile) as a
    virtual origin/destination.
    """

    zones = ListParameter(
        "od_matrix_travel_times.zones_levels",
        inner=Int(),
        min_length=1,
        max_length=2,
        description=(
            "differents zones levels where we want to find od free-flow travel time"
        ),
        default=[4],
        example="[3, 4]"

    )
    input_files = {
        "zone1_road_node": InputFile(ZonesLevel1RoadNodeFile, when=lambda step: 1 in step.zones),
        "zone2_road_node": InputFile(ZonesLevel2RoadNodeFile, when=lambda step: 2 in step.zones),
        "zone3_road_node": InputFile(ZonesLevel3RoadNodeFile, when=lambda step: 3 in step.zones),
        "zone4_road_node": InputFile(ZonesLevel4RoadNodeFile, when=lambda step: 4 in step.zones),
        "zone5_road_node": InputFile(ZonesLevel5RoadNodeFile, when=lambda step: 5 in step.zones),
        "edges": RoadEdgesCleanFile,
        "edges_fftt": RoadEdgesFreeFlowTravelTimeFile
    }
    output_files = {
        "zone1_fftt": ZoneODLevel1FreeFlowTravelTimesFile,
        "zone2_fftt": ZoneODLevel2FreeFlowTravelTimesFile,
        "zone3_fftt": ZoneODLevel3FreeFlowTravelTimesFile,
        "zone4_fftt": ZoneODLevel4FreeFlowTravelTimesFile,
        "zone5_fftt": ZoneODLevel5FreeFlowTravelTimesFile
    }

    def run(self):
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
            logger.warning(f"Discarding {n} edges with NULL free-flow travel time")
            edges = edges.filter(pl.col("weight").is_not_null())

        for zone in self.zones:
            zones_df = (
                self.input[f"zone{zone}_road_node"]
                .read()
                .select("zone_id", "road_node")
            )
            pairs, trips = read_trips(zones_df)
            results = trip_routing(trips, edges, self.exec_path, with_routes=False)
            results = results.select(
                query_id="trip_id",
                free_flow_travel_time=pl.duration(seconds="value")
            )
            df = pairs.join(
                results,
                on="query_id",
                how="left",
                coalesce=False,
            ).select(
                "origin_zone_id",
                "destination_zone_id",
                "free_flow_travel_time"
            )
            self.output[f"zone{zone}_fftt"].write(df)

class ZonesODCongestedTravelTimesStep(RoutingCLIStep):
    """
    Computes the congested travel time by car between each ordered pair of zones,
    using each zone's representative road node as a virtual origin/destination.

    Unlike the free-flow variant, results depend on departure time: routing is
    run once per zone pair againt the congested edge travel-time functions
    produced by the simulation (MetroNextExpectedTravlTimeFunctionsFile),
    giving one breakpoint every recording_interval. The stored value is the median
    travel time over breakpoints falling within time_window (along with the min,
    max and standard deviation over the same breakpoints).

    Some zone pairs have no breakpoint within time_window (e.g. an edge on
    their route was never travelled by a simulated agent during that window,
    which happens disproportionately for zones with little simulated traffic
    such as newly-added cross-border zones). For those pairs, all four
    statistics fall back to the free-flow travel time between the two zones,
    with a standard deviation of zero.
    """
    time_window = ListParameter(
        "od_matrix_travel_times.time_window",
        inner=Time(),
        length=2,
        description=(
            "Time window over which the congested travel time is aggregated"
        ),
        example="[06:00:00, 09:00:00]",
    )
    zones = ListParameter(
        "od_matrix_travel_times.zones_levels",
        inner=Int(),
        min_length=1,
        max_length=5,
        description=(
            "differents zones levels where we want to find congested od travel time"
        ),
        default=[4],
        example="[3, 4]"
    )

    input_files = {
        "zone1_road_node": InputFile(ZonesLevel1RoadNodeFile, when=lambda step: 1 in step.zones),
        "zone2_road_node": InputFile(ZonesLevel2RoadNodeFile, when=lambda step: 2 in step.zones),
        "zone3_road_node": InputFile(ZonesLevel3RoadNodeFile, when=lambda step: 3 in step.zones),
        "zone4_road_node": InputFile(ZonesLevel4RoadNodeFile, when=lambda step: 4 in step.zones),
        "zone5_road_node": InputFile(ZonesLevel5RoadNodeFile, when=lambda step: 5 in step.zones),
        "edges": RoadEdgesCleanFile,
        "edges_fftt": RoadEdgesFreeFlowTravelTimeFile,
        "edge_ttfs": MetroNextExpectedTravelTimeFunctionsFile,
        "zone1_fftt": InputFile(ZoneODLevel1FreeFlowTravelTimesFile,
                                when=lambda step: 1 in step.zones),
        "zone2_fftt": InputFile(ZoneODLevel2FreeFlowTravelTimesFile,
                                when=lambda step: 2 in step.zones),
        "zone3_fftt": InputFile(ZoneODLevel3FreeFlowTravelTimesFile,
                                when=lambda step: 3 in step.zones),
        "zone4_fftt": InputFile(ZoneODLevel4FreeFlowTravelTimesFile,
                                when=lambda step: 4 in step.zones),
        "zone5_fftt": InputFile(ZoneODLevel5FreeFlowTravelTimesFile,
                                when=lambda step: 5 in step.zones)
    }
    output_files = {
        "zone1_congested": ZoneODLevel1CongestedTravelTimesFile,
        "zone2_congested": ZoneODLevel2CongestedTravelTimesFile,
        "zone3_congested": ZoneODLevel3CongestedTravelTimesFile,
        "zone4_congested": ZoneODLevel4CongestedTravelTimesFile,
        "zone5_congested": ZoneODLevel5CongestedTravelTimesFile,
    }
    def run(self):
        assert self.exec_path is not None
        assert self.time_window is not None
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
            logger.warning(f"Discarding {n} edges with NULL free-flow travel time")
            edges = edges.filter(pl.col("weight").is_not_null())

        edge_ttfs = self.input["edge_ttfs"].read()
        edge_ttfs = (
            edge_ttfs
            .filter(pl.col("vehicle_id") == "car_driver_alone")
            .select(
                "edge_id",
                "departure_time",
                "travel_time",
            )
            .join(edges.select("edge_id"), on="edge_id", how="semi")
        )

        for zone in self.zones:
            zones_df = (
                self.input[f"zone{zone}_road_node"]
                .read()
                .select("zone_id", "road_node")
            )
            pairs, trips = read_trips(zones_df)
            with tempfile.TemporaryDirectory() as tmp_directory:
                processing_routing(trips, edges, self.exec_path, edge_ttfs, tmp_directory)
                df = pl.read_parquet(os.path.join(tmp_directory, "output",
                                                  "profile_results.parquet"))
            df = df.filter(
                (self.time_window[0].seconds() <= pl.col("departure_time")) &
                (self.time_window[1].seconds() >= pl.col("departure_time"))
            )
            results = df.group_by("query_id").agg(
                congested_travel_time=pl.col("travel_time").median(),
                congested_travel_time_min=pl.col("travel_time").min(),
                congested_travel_time_max=pl.col("travel_time").max(),
                congested_travel_time_std=pl.col("travel_time").std(),
            )
            zones_fftt = self.input[f"zone{zone}_fftt"].read().with_columns(
                pl.col("free_flow_travel_time").dt.total_nanoseconds() / 1e9
            )
            df = (
                pairs.join(results, on="query_id", how="left", coalesce=False)
                .join(
                    zones_fftt,
                    on=["origin_zone_id", "destination_zone_id"],
                    how="left",
                    coalesce=True,
                )
                .with_columns(
                    congested_travel_time=pl.coalesce(
                        "congested_travel_time", "free_flow_travel_time"
                    ),
                    congested_travel_time_min=pl.coalesce(
                        "congested_travel_time_min", "free_flow_travel_time"
                    ),
                    congested_travel_time_max=pl.coalesce(
                        "congested_travel_time_max", "free_flow_travel_time"
                    ),
                    # Also fills the single-breakpoint case (std of one value is
                    # undefined in polars, not just the no-breakpoint fallback).
                    congested_travel_time_std=pl.col("congested_travel_time_std").fill_null(0.0),
                )
                .select(
                    "origin_zone_id",
                    "destination_zone_id",
                    congested_travel_time=pl.duration(seconds="congested_travel_time"),
                    congested_travel_time_min=pl.duration(seconds="congested_travel_time_min"),
                    congested_travel_time_max=pl.duration(seconds="congested_travel_time_max"),
                    congested_travel_time_std=pl.duration(seconds="congested_travel_time_std"),
                )
            )
            self.output[f"zone{zone}_congested"].write(df)


def read_trips(zones: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Builds the zone x zone virtual "trips" used as OD queries for routing:
    every ordered pair of distinct zones, using each zone's representative road
    node as origin/destination.

    Returns two views of the same pairs: 'pairs' keeps the zone ids (used to join
    the routing results back to zone ids once routing is done),'trips' keeps only
    the node ids, renamed to the format expected by 'trip_routing()/processing_routing()'
    ('trip_id', 'origin_node', 'destination_node')
    """
    pairs = zones.select(origin_zone_id="zone_id",
                         origin_node="road_node").join(
                             zones.select(
                                 destination_zone_id="zone_id",
                                 destination_node="road_node",
                            ),
                             how="cross"
                         )
    pairs = (
        pairs
        .filter(pl.col("origin_zone_id") != pl.col("destination_zone_id"))
        .with_columns(query_id=pl.int_range(0, pl.len(), dtype=pl.UInt64))
    )
    trips = pairs.select(
        "query_id",
        "origin_node",
        "destination_node",
    ).rename({"query_id": "trip_id"})
    return pairs, trips


def processing_routing(
    trips: pl.DataFrame,
    edges: pl.DataFrame,
    routing_exec_path: Path,
    edge_ttfs: pl.DataFrame,
    tmp_directory: str
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
    queries = trips.select(
        query_id="trip_id",
        origin="origin_node",
        destination="destination_node",
        departure_time=pl.lit(None, dtype=pl.Float64)
    )
    queries.write_parquet(os.path.join(tmp_directory, "queries.parquet"))

    edges = edges.select("edge_id", "source", "target", "weight")
    edges = edges.sort("weight").unique(subset=["source", "target"], keep="first").sort("edge_id")
    edges.rename({"weight": "travel_time"}).write_parquet(
        os.path.join(tmp_directory, "edges.parquet")
    )

    edge_ttfs.write_parquet(
        os.path.join(tmp_directory, "edge_ttfs.parquet")
    )

    parameters = {
        "algorithm": "Intersect",
        "output_route": False,
        "input_files": {
            "queries": "queries.parquet",
            "edges": "edges.parquet",
            "edge_ttfs": "edge_ttfs.parquet"
        },
        "output_directory": "output",
        "saving_format": "Parquet",
    }
    with open(os.path.join(tmp_directory, "parameters.json"), "w") as f:
        json.dump(parameters, f)
    run_routing(routing_exec_path, tmp_directory)
