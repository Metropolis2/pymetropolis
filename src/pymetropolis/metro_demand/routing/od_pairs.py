from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_demand.population.files import TripsDestinationsFile, TripsOriginsFile
from pymetropolis.metro_demand.routing.files import TripsPedestrianNodesFile, TripsRoadNodesFile
from pymetropolis.metro_network.pedestrian_network.files import PedestrianEdgesCleanFile
from pymetropolis.metro_network.road_network.files import RoadEdgesCleanFile
from pymetropolis.metro_pipeline.parameters import ListParameter
from pymetropolis.metro_pipeline.types import String
from pymetropolis.metro_spatial import GeoStep

if TYPE_CHECKING:
    import geopandas as gpd
    import polars as pl


def identify_od_pairs(
    edges: gpd.GeoDataFrame, origins_gdf: gpd.GeoDataFrame, destinations_gdf: gpd.GeoDataFrame
) -> pl.DataFrame:
    """Identify the origin and destination network node from origin / destination coordinates."""
    import polars as pl
    from shapely.geometry import Point

    assert len(origins_gdf) == len(destinations_gdf)
    # Create source / target point of the edges.
    logger.debug("Creating source / target points")
    # TODO: Speed-up this with duckdb
    edges["source_point"] = edges["geometry"].apply(lambda g: Point(g.coords[0]))
    edges["target_point"] = edges["geometry"].apply(lambda g: Point(g.coords[-1]))
    logger.debug("Identifying nearest nodes for origins")
    origins = identify_nodes(edges, origins_gdf)
    logger.debug("Identifying nearest nodes for destinations")
    destinations = identify_nodes(edges, destinations_gdf)
    origins = origins.select("trip_id", pl.all().exclude("trip_id").name.prefix("origin_"))
    destinations = destinations.select(
        "trip_id", pl.all().exclude("trip_id").name.prefix("destination_")
    )
    df = origins.join(destinations, on="trip_id")
    assert len(df) == len(origins_gdf)
    return df


def identify_nodes(edges: gpd.GeoDataFrame, nodes_gdf: gpd.GeoDataFrame) -> pl.DataFrame:
    """Identify the closest edge for each node in a list."""
    import polars as pl

    assert edges.crs == nodes_gdf.crs, "Mis-matched CRS between edges and nodes"
    # Match to the nearest edge.
    nodes_gdf = nodes_gdf.sjoin_nearest(
        edges[["edge_id", "geometry", "source", "target", "source_point", "target_point"]],
        distance_col="edge_dist",
        how="left",
    )
    # Duplicate indices occur when there are two edges at the same distance.
    nodes_gdf.drop_duplicates(subset=["trip_id"], inplace=True)
    # Compute distance to the source / target node of nearest edge.
    nodes_gdf["source_dist"] = nodes_gdf["geometry"].distance(nodes_gdf["source_point"])
    nodes_gdf["target_dist"] = nodes_gdf["geometry"].distance(nodes_gdf["target_point"])
    # Set the nearest node.
    nodes = pl.from_pandas(
        nodes_gdf.loc[
            :, ["trip_id", "edge_id", "edge_dist", "source", "target", "source_dist", "target_dist"]
        ]
    )
    mask = pl.col("source_dist") > pl.col("target_dist")
    nodes = nodes.with_columns(
        node=pl.when(mask).then("target").otherwise("source"),
        node_dist=pl.when(mask).then("target_dist").otherwise("source_dist"),
    )
    # Compute the projected node dist on edge from Pythagorean Theorem.
    nodes = nodes.with_columns(
        node_dist_on_edge=(pl.col("node_dist") ** 2 - pl.col("edge_dist") ** 2).sqrt()
    )
    nodes = nodes.select(
        "trip_id", "node", "node_dist", "node_dist_on_edge", "edge_dist", edge="edge_id"
    )
    return nodes


class PedestrianODNodesFromCoordinatesStep(GeoStep):
    """Identifies nodes on the pedestrian network to be used as origins and destinations of the
    trips.

    First, this Step finds the nearest edge to the origin / destination coordinates.
    Edges whose type is specified in the
    [`forbidden_types`](parameters.md#pedestrian_networkforbidden_types) parameter are excluded from
    that search.
    Then, the origin / destination node is either the source or target of that nearest edge,
    whichever is closer.
    """

    forbidden_types = ListParameter(
        "pedestrian_network.forbidden_types",
        inner=String(),
        default=[],
        description=(
            "List of pedestrian edges' types that *cannot* be used as origin / destination edge."
        ),
        example='`["trunk", "trunk_link"]`',
    )
    input_files = {
        "edges": PedestrianEdgesCleanFile,
        "origins": TripsOriginsFile,
        "destinations": TripsDestinationsFile,
    }
    output_files = {"ods": TripsPedestrianNodesFile}

    def run(self):
        import polars as pl

        edges = self.input["edges"].read()
        edges = edges.loc[
            ~edges["edge_type"].isin(self.forbidden_types),
            ["edge_id", "geometry", "source", "target"],
        ]
        origins = self.input["origins"].read()
        destinations = self.input["destinations"].read()
        ods = identify_od_pairs(edges, origins, destinations)
        ods = ods.select(
            pl.all()
            .name.replace("origin_", "origin_pedestrian_")
            .name.replace("destination_", "destination_pedestrian_")
        )
        self.output["ods"].write(ods)


class RoadODNodesFromCoordinatesStep(GeoStep):
    """Identifies nodes on the road network to be used as origins and destinations of the trips.

    First, this Step finds the nearest edge to the origin / destination coordinates.
    Edges whose type is specified in the
    [`forbidden_types`](parameters.md#road_networkforbidden_types) parameter are excluded from that
    search.
    Then, the origin / destination node is either the source or target of that nearest edge,
    whichever is closer.
    """

    forbidden_types = ListParameter(
        "road_network.forbidden_types",
        inner=String(),
        default=[],
        description=(
            "List of road edges' types that *cannot* be used as origin / destination edge."
        ),
        example='`["motorway", "motorway_link", "trunk", "trunk_link"]`',
    )
    input_files = {
        "edges": RoadEdgesCleanFile,
        "origins": TripsOriginsFile,
        "destinations": TripsDestinationsFile,
    }
    output_files = {"ods": TripsRoadNodesFile}

    def run(self):
        import polars as pl

        edges = self.input["edges"].read()
        edges = edges.loc[
            ~edges["edge_type"].isin(self.forbidden_types),
            ["edge_id", "geometry", "source", "target"],
        ]
        origins = self.input["origins"].read()
        destinations = self.input["destinations"].read()
        ods = identify_od_pairs(edges, origins, destinations)
        ods = ods.select(
            pl.all()
            .name.replace("origin_", "origin_road_")
            .name.replace("destination_", "destination_road_")
        )
        self.output["ods"].write(ods)
