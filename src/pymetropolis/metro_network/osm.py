from pathlib import Path
from typing import Any

import geopandas as gpd
import osmium
import polars as pl
import pyproj
from loguru import logger
from osmium import IdTracker
from osmium.filter import EntityFilter, TagFilter
from osmium.geom import WKBFactory
from osmium.osm import NODE, WAY, Node, Way
from shapely.geometry import LineString, Polygon

from pymetropolis.metro_common import MetropyError


class OpenStreetMapNetworkImport:
    """Generic class to import a network from OpenStreetMap data.

    Parameters
    ----------
    - osm_file: Path to a OSM file that can be read by osmium.
    - highway_tags: list of `highway=*` values that define valid ways for the network.
    - crs: projected CRS to be used for geometric operations, must be a valid pyproj CRS.
    - filter_polygon: optional polygon to filter ways, must be in the same CRS.
    """

    def __init__(
        self,
        osm_file: Path,
        highway_tags: list[str],
        crs: pyproj.CRS,
        filter_polygon: Polygon | None,
    ):
        self.osm_file = osm_file
        self.highway_tags = highway_tags
        self.crs = crs
        self.filter_polygon = filter_polygon

    def run(self):
        """Runs all operations required to import the network and returns a GeoDataFrame of edges
        with their characteristics.
        """
        way_id_tracker = self.filter_highway_ways()
        edges, node_id_tracker = self.read_highway_ways(way_id_tracker)
        nodes = self.read_highway_nodes(node_id_tracker)
        edges_gdf = self.create_edges(edges, nodes)
        return edges_gdf

    def filter_highway_ways(self) -> IdTracker:
        """Reads all the ways in the OSM file and returns a IdTracker with the id of all the valid
        way.

        A way is valid if:
        - It has a valid highway tag.
        - Its geometry is valid.
        - It intersects with the filtering polygon (if any).
        - It is valid according to the `extra_way_filter` method.
        """
        logger.info("Filtering highway ways")
        ids = list()
        linestrings = list()
        fab = WKBFactory()
        valid_tag_pairs = tuple(("highway", tag) for tag in self.highway_tags)
        logger.debug("Reading ways from OSM file")
        for way in (
            osmium.FileProcessor(self.osm_file)
            .with_filter(EntityFilter(WAY))
            .with_filter(TagFilter(*valid_tag_pairs))
            .with_locations()
        ):
            if not self.is_valid_way(way) and not self.extra_way_filter(way):  # ty: ignore[invalid-argument-type]
                continue
            ids.append(way.id)
            linestrings.append(fab.create_linestring(way.nodes))  # ty: ignore[unresolved-attribute]
        if not ids:
            raise MetropyError("No valid way in the OSM data")
        logger.debug("Building GeoDataFrame")
        gdf = gpd.GeoDataFrame(
            {"id": ids}, geometry=gpd.GeoSeries.from_wkb(linestrings, crs="EPSG:4326")
        )
        logger.debug("Converting to required CRS")
        gdf.to_crs(self.crs, inplace=True)
        if self.filter_polygon is not None:
            logger.debug("Filtering based on area")
            mask = [self.filter_polygon.intersects(geom) for geom in gdf.geometry]
            gdf = gdf.loc[mask].copy()
            if len(gdf) == 0:
                raise MetropyError("The simulation area does not intersect with the OSM data")
        logger.debug("Creating id tracker")
        tracker = IdTracker()
        for way_id in gdf["id"].values:
            tracker.add_way(way_id)
        return tracker

    def is_valid_way(self, way: Way) -> bool:
        """Returns True if the candidate way has a valid geometry."""
        return len(way.nodes) >= 2 and not way.is_closed()

    def extra_way_filter(self, way: Way) -> bool:
        """Returns True if the candidate way should be imported."""
        return True

    def way_data(self, way: Way) -> dict[str, Any]:
        """Returns a dictionary with the relevant OpenStreetMap data to extract from a valid way."""
        return {
            "osm_id": way.id,
            "nodes": tuple(n.ref for n in way.nodes),
            "road_type": way.tags["highway"],
            "name": way.tags.get("name") or way.tags.get("addr:street") or way.tags.get("ref"),
        }

    def way_data_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        """Returns a dictionary representing the Polars schema for the DataFrame constructed from
        `way_data`."""
        return {
            "osm_id": pl.UInt64,
            "nodes": pl.List(pl.UInt64),
            "road_type": pl.String,
            "name": pl.String,
        }

    def split_intersected_ways(self, df: pl.DataFrame) -> pl.DataFrame:
        """Split ways at the node where they are intersected by another way."""
        # The intersection nodes are the nodes which are source or target of a way (first or last
        # node) or which appears twice in the data (thus representing an intersection between two
        # ways).
        duplicate_nodes = set(
            df["nodes"].explode().value_counts().filter(pl.col("count") > 1)["nodes"]
        )
        source_target_nodes = set(df["nodes"].list.first()) | set(df["nodes"].list.last())
        intersection_nodes = duplicate_nodes | source_target_nodes
        # Select only the intersection nodes in the `nodes` column.
        lf = df.lazy()
        lf = lf.with_columns(
            main_nodes=pl.col("nodes")
            .list.eval(
                pl.when(pl.element().is_in(intersection_nodes))
                .then(pl.struct(node=pl.element(), idx=pl.int_range(pl.len())))
                .otherwise(pl.lit(None))
            )
            .list.drop_nulls(),
            node_idx=pl.col("nodes").list.eval(pl.int_range(pl.len())),
        )
        # Add source and target column, while duplicating rows when ways need to be split.
        lf = (
            lf.explode("main_nodes")
            .with_columns(
                source=pl.col("main_nodes").struct.field("node").shift(1).over("osm_id"),
                source_idx=pl.col("main_nodes").struct.field("idx").shift(1).over("osm_id"),
                target=pl.col("main_nodes").struct.field("node"),
                target_idx=pl.col("main_nodes").struct.field("idx"),
            )
            .with_columns(
                nodes=pl.col("nodes").list.slice(
                    pl.col("source_idx"), pl.lit(1) + pl.col("target_idx") - pl.col("source_idx")
                )
            )
            .drop_nulls("source")
        )
        return lf.collect()  # ty: ignore[invalid-return-type]

    def clean_way_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Returns a cleaned version of data collected from `way_data`."""
        # Drop rows with source == target.
        df = df.filter(pl.col("source") != pl.col("target"))
        df: pl.DataFrame = df.select("osm_id", "source", "target", "road_type", "name", "nodes")
        return df

    def read_highway_ways(self, way_id_tracker: IdTracker) -> tuple[pl.DataFrame, IdTracker]:
        """Reads all the ways in the OSM file with a valid id and returns a DataFrame with their
        characteristics and an IdTracker with their node ids.
        """
        logger.info("Reading highway ways")
        data = list()
        logger.debug("Reading ways from OSM file")
        for way in osmium.FileProcessor(self.osm_file, WAY).with_filter(way_id_tracker.id_filter()):
            data.append(self.way_data(way))  # ty: ignore[invalid-argument-type]
        logger.debug("Building DataFrame")
        df = pl.DataFrame(data, schema_overrides=self.way_data_schema())
        logger.debug("Spliting ways at intersections")
        df = self.split_intersected_ways(df)
        logger.debug("Cleaning way data")
        df = self.clean_way_data(df)
        # Keep track of all nodes to later identify the highway nodes.
        all_nodes = set(df["nodes"].explode())
        node_id_tracker = IdTracker()
        for n in all_nodes:
            node_id_tracker.add_node(n)
        return df, node_id_tracker

    def node_data(self, node: Node) -> dict[str, Any]:
        """Returns a dictionary with the relevant OpenStreetMap data to extract from a node of the
        network.
        """
        return {"osm_id": node.id, "lat": node.lat, "lon": node.lon}

    def node_data_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        """Returns a dictionary representing the Polars schema for the DataFrame constructed from
        `node_data`.
        """
        return {"osm_id": pl.UInt64, "lat": pl.Float64, "lon": pl.Float64}

    def read_highway_nodes(self, node_id_tracker: IdTracker) -> pl.DataFrame:
        """Reads all the nodes in the OSM file with a valid id and returns a DataFrame with their
        characteristics (including coordinates).
        """
        logger.info("Reading highway nodes")
        data = list()
        logger.debug("Reading nodes from OSM file")
        for node in osmium.FileProcessor(self.osm_file, NODE).with_filter(
            node_id_tracker.id_filter()
        ):
            data.append(self.node_data(node))  # ty: ignore[invalid-argument-type]
        logger.debug("Building DataFrame")
        df = (
            pl.DataFrame(data, schema_overrides=self.node_data_schema())
            .with_columns(coords=pl.struct("lon", "lat"))
            .drop("lat", "lon")
        )
        return df

    def add_node_features_to_edges(self, edges: pl.DataFrame, nodes: pl.DataFrame) -> pl.DataFrame:
        """Returns edges with additional informations read from node data (e.g., traffic signals,
        stop signs).
        """
        return edges

    def duplicate_edges(self, edges: pl.DataFrame) -> pl.DataFrame:
        """Duplicates edges in the forward and backward direction.

        By default, all edges are duplicated (appropriate for pedestrian networks), but this method
        can be override to only duplicate edges which can be taken in both directions.
        """
        forward_edges = edges.lazy().with_columns(backward=False)
        backward_edges = edges.lazy().with_columns(
            target="source", source="target", nodes=pl.col("nodes").list.reverse(), backward=True
        )
        return pl.concat((forward_edges, backward_edges), how="vertical").collect()  # ty: ignore[invalid-return-type]

    def edge_columns(self) -> list[str]:
        """Returns a list of columns to be kept in the final edge DataFrame."""
        return ["edge_id", "source", "target", "road_type", "name", "nodes", "original_id"]

    def create_edges(self, edges: pl.DataFrame, nodes: pl.DataFrame) -> gpd.GeoDataFrame:
        """Creates edge geometries from node coordinates and duplicate the two-way edges."""
        edges = self.add_node_features_to_edges(edges, nodes)
        logger.debug("Duplicating two-way edges")
        edges = self.duplicate_edges(edges)
        # Create `edge_id` column.
        # For forward edges: `{osm_id}`
        # For backward edges: `{osm_id}r`
        # For split edges (forward): `{osm_id}-{i}` (with i the split index)
        # For split edges (backward): `{osm_id}r-{i}` (with i the split index)
        edges = (
            edges.with_columns(
                # Edge has been split if there are at least two "forward" edges with the same
                # osm_id.
                split=pl.col("backward").not_().sum().over("osm_id") > 1,
                bwd_symbol=pl.when("backward").then(pl.lit("r")).otherwise(pl.lit("")),
            )
            .with_columns(edge_id=pl.format("{}{}", "osm_id", "bwd_symbol"))
            .with_columns(
                edge_id=pl.when("split")
                .then(
                    pl.format("{}-{}", "edge_id", pl.int_range(pl.len()).over("osm_id", "backward"))
                )
                .otherwise("edge_id"),
                original_id="osm_id",
            )
        )
        edges = edges.sort("osm_id", "backward", "source")
        edges = edges.select(self.edge_columns())
        logger.debug("Creating edge geometries")
        edge_coords = edges["nodes"].list.eval(
            pl.element().replace_strict(nodes["osm_id"], nodes["coords"])
        )
        np_edge_coords = [coords.to_numpy() for coords in edge_coords]
        geoms = [LineString(coords) for coords in np_edge_coords]
        edges = edges.drop("nodes")
        gdf = gpd.GeoDataFrame(edges.to_pandas(), geometry=gpd.GeoSeries(geoms, crs="EPSG:4326"))
        logger.debug("Converting to required CRS")
        gdf.to_crs(self.crs, inplace=True)
        logger.debug("Computing edges' length")
        gdf["length"] = gdf.geometry.length
        return gdf
