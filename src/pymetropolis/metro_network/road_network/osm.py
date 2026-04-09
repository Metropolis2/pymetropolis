from pathlib import Path

import geopandas as gpd
import osmium
import polars as pl
import pyproj
from loguru import logger
from osmium import IdTracker
from osmium.filter import EntityFilter, TagFilter
from osmium.geom import WKBFactory
from osmium.osm import NODE, WAY
from shapely.geometry import LineString, Polygon

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_pipeline.parameters import ListParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_pipeline.types import String
from pymetropolis.metro_spatial import GeoStep, OSMStep
from pymetropolis.metro_spatial.simulation_area.file import SimulationAreaFile

from .files import RawEdgesFile

# Dictionary for special `maxspeed` values.
SPEED_DICT = {"walk": 8, "FR:walk": 20, "FR:urban": 50, "FR:rural": 80}

# Conversion miles to kilometers.
M_TO_KM = 1.609344


def import_osm_road_network(
    osm_file: Path,
    highways: list[str],
    crs: pyproj.CRS,
    simulation_area_file: SimulationAreaFile,
    allowed_access: list[str],
):
    filter_polygon = simulation_area_file.get_area_opt()
    way_id_tracker = filter_highway_ways(osm_file, highways, crs, filter_polygon, allowed_access)
    edges, node_id_tracker = read_highway_ways(osm_file, way_id_tracker)
    nodes = read_highway_nodes(osm_file, node_id_tracker)
    edges_gdf = create_edges(edges, nodes, crs)
    return edges_gdf


def filter_highway_ways(
    osm_filename: Path,
    highways: list[str],
    crs: pyproj.CRS,
    filter_polygon: Polygon | None,
    allowed_access: list[str],
) -> IdTracker:
    """Reads all the ways in the OSM file and returns a IdTracker with the id of all the valid way.

    A way is valid if:
    - It has a valid highway tag.
    - It has not access tag restricting the access for cars.
    - It is not an area.
    - Its geometry is valid.
    - It intersects with the filtering polygon (if any).
    """
    logger.info("Filtering highway ways")
    ids = list()
    linestrings = list()
    fab = WKBFactory()
    valid_tag_pairs = tuple(("highway", tag) for tag in highways)
    logger.debug("Reading ways from OSM file")
    for way in (
        osmium.FileProcessor(osm_filename)
        .with_filter(EntityFilter(WAY))
        .with_filter(TagFilter(*valid_tag_pairs))
        .with_locations()
    ):
        if not is_valid_way(way, set(allowed_access)):
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
    gdf.to_crs(crs, inplace=True)
    if filter_polygon is not None:
        logger.debug("Filtering based on area")
        mask = [filter_polygon.intersects(geom) for geom in gdf.geometry]
        gdf = gdf.loc[mask].copy()
        if len(gdf) == 0:
            raise MetropyError("The simulation area does not intersect with the OSM data")
    logger.debug("Creating id tracker")
    tracker = IdTracker()
    for way_id in gdf["id"].values:
        tracker.add_way(way_id)
    return tracker


def is_valid_way(way, allowed_access: set[str]):
    has_access = "access" not in way.tags or way.tags["access"] in allowed_access
    return has_access and len(way.nodes) >= 2 and not way.is_closed()


def read_highway_ways(
    osm_filename: Path, way_id_tracker: IdTracker
) -> tuple[pl.DataFrame, IdTracker]:
    """Reads all the ways in the OSM file with a valid id and returns a DataFrame with their
    characteristics and an IdTracker with their node ids.
    """
    logger.info("Reading highway ways")
    data = list()
    logger.debug("Reading ways from OSM file")
    for way in osmium.FileProcessor(osm_filename, WAY).with_filter(way_id_tracker.id_filter()):
        data.append(
            {
                "osm_id": way.id,
                "nodes": tuple(n.ref for n in way.nodes),  # ty: ignore[unresolved-attribute]
                "road_type": way.tags["highway"],
                "name": way.tags.get("name") or way.tags.get("addr:street") or way.tags.get("ref"),
                "toll": way.tags.get("toll") == "yes",
                "roundabout": way.tags.get("junction") == "roundabout",
                "oneway": way.tags.get("oneway") == "yes",
                "maxspeed": way.tags.get("maxspeed"),
                "maxspeed:forward": way.tags.get("maxspeed:forward"),
                "maxspeed:backward": way.tags.get("maxspeed:backward"),
                "lanes": way.tags.get("lanes"),
                "lanes:forward": way.tags.get("lanes:forward"),
                "lanes:backward": way.tags.get("lanes:backward"),
            }
        )
    logger.debug("Building DataFrame")
    df = pl.DataFrame(
        data,
        schema_overrides={
            "osm_id": pl.UInt64,
            "nodes": pl.List(pl.UInt64),
            "road_type": pl.String,
            "name": pl.String,
            "toll": pl.Boolean,
            "roundabout": pl.Boolean,
            "oneway": pl.Boolean,
            "maxspeed": pl.String,
            "maxspeed:forward": pl.String,
            "maxspeed:backward": pl.String,
            "lanes": pl.String,
            "lanes:forward": pl.String,
            "lanes:backward": pl.String,
        },
    )
    logger.debug("Identifying intersection nodes")
    # The intersection nodes are the nodes which are source or target of a way (first or last node)
    # or which appears twice in the data (thus representing an intersection between two ways).
    duplicate_nodes = set(df["nodes"].explode().value_counts().filter(pl.col("count") > 1)["nodes"])
    source_target_nodes = set(df["nodes"].list.first()) | set(df["nodes"].list.last())
    intersection_nodes = duplicate_nodes | source_target_nodes
    # Keep track of all nodes to later identify the highway nodes.
    all_nodes = set(df["nodes"].explode())
    node_id_tracker = IdTracker()
    for n in all_nodes:
        node_id_tracker.add_node(n)
    logger.debug("Cleaning way data")
    lf = df.lazy()
    # Roundabouts are oneway.
    lf = lf.with_columns(oneway=pl.col("oneway").or_(pl.col("roundabout")))
    # Find maximum speed if available.
    # Special values are handled by `SPEED_DICT`.
    # Values in mph, like "30 mph", are converted to km/h.
    lf = lf.with_columns(
        pl.col(col)
        .str.extract("([0-9]+) mph")
        .cast(pl.Float64, strict=False)
        .mul(M_TO_KM)
        .fill_null(pl.col(col).replace(SPEED_DICT).cast(pl.Float64, strict=False))
        for col in ("maxspeed", "maxspeed:forward", "maxspeed:backward")
    ).with_columns(
        # Read tags maxspeed:forward and maxspeed:backward when available, otherwise read tag
        # maxspeed.
        forward_speed_limit=pl.col("maxspeed:forward").fill_null(pl.col("maxspeed")),
        backward_speed_limit=pl.when("oneway")
        .then(pl.lit(None))
        .otherwise(pl.col("maxspeed:backward").fill_null(pl.col("maxspeed"))),
    )
    # Find number of lanes if available.
    # Read tags lanes:forward and lanes:backward when available, otherwise read tag lanes.
    lf = lf.with_columns(
        pl.col("lanes").cast(pl.Float64, strict=False),
        pl.col("lanes:forward").cast(pl.Float64, strict=False),
        pl.col("lanes:backward").cast(pl.Float64, strict=False),
    ).with_columns(
        forward_lanes=pl.when("oneway")
        .then(pl.col("lanes:forward").fill_null(pl.col("lanes")))
        .otherwise(pl.col("lanes:forward").fill_null(pl.col("lanes") / 2.0)),
        backward_lanes=pl.when("oneway")
        .then(pl.lit(None))
        .otherwise(pl.col("lanes:backward").fill_null(pl.col("lanes") / 2.0)),
    )
    # Select only the intersection nodes in the `nodes` column.
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
    # Drop rows with source == target.
    lf = lf.filter(pl.col("source") != pl.col("target"))
    df: pl.DataFrame = lf.select(
        "osm_id",
        "source",
        "target",
        "road_type",
        "name",
        "toll",
        "roundabout",
        "oneway",
        "forward_speed_limit",
        "backward_speed_limit",
        "forward_lanes",
        "backward_lanes",
        "nodes",
    ).collect()  # ty: ignore[invalid-assignment]
    return df, node_id_tracker


def read_highway_nodes(osm_filename: Path, node_id_tracker: IdTracker) -> pl.DataFrame:
    """Reads all the nodes in the OSM file with a valid id and returns a DataFrame with their
    characteristics (including coordinates)."""
    logger.info("Reading highway nodes")
    data = list()
    logger.debug("Reading nodes from OSM file")
    for node in osmium.FileProcessor(osm_filename, NODE).with_filter(node_id_tracker.id_filter()):
        data.append(
            {
                "osm_id": node.id,
                "lat": node.lat,  # ty: ignore[unresolved-attribute]
                "lon": node.lon,  # ty: ignore[unresolved-attribute]
                "highway": node.tags.get("highway"),
                "direction": node.tags.get("traffic_signals:direction")
                or node.tags.get("direction"),
            }
        )
    logger.debug("Building DataFrame")
    df = (
        pl.DataFrame(
            data,
            schema_overrides={
                "osm_id": pl.UInt64,
                "lat": pl.Float64,
                "lon": pl.Float64,
                "highway": pl.String,
                "direction": pl.String,
            },
        )
        .with_columns(coords=pl.struct("lon", "lat"))
        .with_columns(
            pl.when(pl.col("direction").is_in(("both", "forward", "backward")))
            .then("direction")
            .otherwise(pl.lit(None))
        )
        .drop("lat", "lon")
    )
    return df


def create_edges(edges: pl.DataFrame, nodes: pl.DataFrame, crs) -> gpd.GeoDataFrame:
    """Creates edge geometries from node coordinates and duplicate the two-way edges."""
    for feature in ("give_way", "stop", "traffic_signals"):
        edges = identify_edge_features(edges, nodes, feature)
    logger.debug("Duplicating two-way edges")
    # Duplicate two-way ways.
    features = ("speed_limit", "lanes", "give_way", "stop", "traffic_signals")
    lf = edges.lazy()
    forward_edges = lf.with_columns(
        *(pl.col(f"forward_{feat}").alias(feat) for feat in features), backward=False
    )
    backward_edges = lf.filter(pl.col("oneway").not_()).with_columns(
        pl.col("source").alias("target"),
        pl.col("target").alias("source"),
        pl.col("nodes").list.reverse(),
        *(pl.col(f"backward_{feat}").alias(feat) for feat in features),
        backward=True,
    )
    lf = pl.concat((forward_edges, backward_edges), how="vertical")
    # Create id column.
    lf = (
        lf.with_columns(
            split=pl.col("backward").not_().sum().over("osm_id") > 1,
            bwd_symbol=pl.when("backward").then(pl.lit("r")).otherwise(pl.lit("")),
        )
        .with_columns(edge_id=pl.format("{}{}", "osm_id", "bwd_symbol"))
        .with_columns(
            edge_id=pl.when("split")
            .then(pl.format("{}-{}", "edge_id", pl.int_range(pl.len()).over("osm_id", "backward")))
            .otherwise("edge_id")
        )
    )
    lf = lf.sort("osm_id", "backward", "source")
    edges = lf.select(
        "edge_id",
        "source",
        "target",
        "road_type",
        "name",
        "toll",
        "roundabout",
        "oneway",
        *features,
        "nodes",
        original_id="osm_id",
    ).collect()  # ty: ignore[invalid-assignment]
    logger.debug("Creating edge geometries")
    edge_coords = edges["nodes"].list.eval(
        pl.element().replace_strict(nodes["osm_id"], nodes["coords"])
    )
    np_edge_coords = [coords.to_numpy() for coords in edge_coords]
    geoms = [LineString(coords) for coords in np_edge_coords]
    edges = edges.drop("nodes")
    gdf = gpd.GeoDataFrame(edges.to_pandas(), geometry=gpd.GeoSeries(geoms, crs="EPSG:4326"))
    logger.debug("Converting to required CRS")
    gdf.to_crs(crs, inplace=True)
    logger.debug("Computing edges' length")
    gdf["length"] = gdf.geometry.length
    return gdf


def identify_edge_features(edges: pl.DataFrame, nodes: pl.DataFrame, feature: str) -> pl.DataFrame:
    """Identifies the edges that contain nodes with a particular feature (e.g., traffic signals,
    stop signs).

    Edges is marked as having the feature in the forward direction if:
    - One of the edge's nodes is marked as having the feature in forward direction.
    - The target node of the edge is marked as having the feature (with no direction specified).
    - The edge is "oneway" and one of its nodes is marked as having the feature (with no direction
      specified).

    Edges is marked as having the feature in the backward direction if:
    - One of the edge's nodes is marked as having the feature in backward direction.
    - The source node of the edge is marked as having the feature (with no direction specified).
    """
    logger.debug(f"Identifying edges with {feature} nodes")
    featured_nodes = nodes.filter(pl.col("highway") == feature)
    fwd_node_ids = featured_nodes.filter(pl.col("direction").is_in(("both", "forward")))["osm_id"]
    bwd_node_ids = featured_nodes.filter(pl.col("direction").is_in(("both", "backward")))["osm_id"]
    no_dir_node_ids = featured_nodes.filter(pl.col("direction").is_null())["osm_id"]
    edges = edges.with_columns(
        (
            pl.col("nodes").list.eval(pl.element().is_in(fwd_node_ids)).list.any()
            | pl.col("target").is_in(no_dir_node_ids)
            | pl.col("oneway").and_(
                pl.col("nodes").list.eval(pl.element().is_in(no_dir_node_ids)).list.any()
            )
        ).alias(f"forward_{feature}"),
        (
            pl.col("nodes").list.eval(pl.element().is_in(bwd_node_ids)).list.any()
            | pl.col("source").is_in(no_dir_node_ids)
        ).alias(f"backward_{feature}"),
    )
    return edges


class OpenStreetMapRoadImportStep(GeoStep, OSMStep):
    """Imports a road network from OpenStreetMap data.

    Edges of the road network are read from the OpenStreetMap ways with tag
    [highway:*](https://wiki.openstreetmap.org/wiki/Key:highway).

    The [`osm_road_import.highways`](parameters.md#osm_road_importhighways) parameter is used to
    define the `highway` values which are part of the road network. For example, values `"motorway"`
    and `"motorway_link"` should usually be included, while value `"footway"` should be excluded as
    it does not represent ways accessible by road vehicles.

    In addition to the `highways` parameter, this step requires both the
    [`osm_file`](parameters.md#osm_file) and [`crs`](parameters.md#crs) parameters to be set.

    For example, to import the major roads of Paris, you can use:

    ```toml
    osm_file = "path/to/paris.osm.pbf"
    crs = "epsg:2154"

    [osm_road_import]
    highways = [
      "motorway",
      "motorway_link",
      "trunk",
      "trunk_link",
      "primary",
      "primary_link",
      "secondary",
      "secondary_link",
    ]
    ```

    An OpenStreetMap way is selected as valid road-network edge if it satisfies all the conditions
    below:

    - Tag `highway` matches one of the value given in
      [`highways`](parameters.md#osm_road_importhighways) parameter.
    - The way has no tag `access` or tag `access` matches one of the value given in
      [`allowed_access`](parameters.md#osm_road_importallowed_access).
    - The way's geometry is a valid LineString.
    - The way intersects with the simulation area (if the
      [SimulationAreaFile](files.md#simulationareafile)) exists).

    By default, the allowed tags for access are `yes`, `permissive` (non-public roads with allowed
    access), and `destination` (allowed for local traffic only).

    Various processing operations are done to clean the edges:

    - Duplicate `oneway=false` ways in two opposing edges.
    - Split ways at nodes where another way is intersecting.

    Edges attributes are defined as follows:

    - `edge_id`: OSM id of the way, with "r" appended if the edge is going backward, with "-[idx]"
      appended if the edge is split in multiple segments (with `idx` the segment index).
    - `source`: OSM id of the source node.
    - `target`: OSM id of the target node.
    - `original_id`: OSM id of the way (note that values are generally not unique).
    - `length`: computed as geometric operation on the ways' LineString, after conversion to the
      simulation CRS.
    - `road_type`: `highway` tag value.
    - `name`: `name` tag value if any, otherwise `addr:street` tag value if any, otherwise `ref` tag
      value if any.
    - `speed_limit`: read from `maxspeed`, `maxspeed:forward`, or `maxspeed:backward` tag, as
      appropriate.
    - `lanes`: read from `lanes`, `lanes:forward`, or `lanes:backward` tag, as appropriate.
    - `oneway`: `oneway` tag value.
    - `roundabout`: `True` if `junction=roundabout`.
    - `toll`: `True` if `toll=yes`.
    - `give_way`: way has a node with `highway=give_way`, in the correct direction.
    - `stop`: way has a node with `highway=stop`, in the correct direction.
    - `traffic_signals`: way has a node with `highway=traffic_signals`, in the correct direction.
    """

    highways = ListParameter(
        "osm_road_import.highways",
        inner=String(),
        min_length=1,
        description="List of `highway=*` OpenStreetMap tags to be considered as valid road ways.",
        example='`["motorway", "motorway_link", "trunk", "trunk_link", "primary", "primary_link"]`',
        note=(
            "A list of highway tags with description is available on the "
            "[OpenStreetMap wiki](https://wiki.openstreetmap.org/wiki/Key:highway)."
        ),
    )
    allowed_access = ListParameter(
        "osm_road_import.allowed_access",
        inner=String(),
        default=["yes", "permissive", "destination"],
        description=(
            "List of `access=*` OpenStreetMap tags defining ways accessible to road vehicles."
        ),
        note=(
            "Any way with an `access` value that is not in the given list will be considered as "
            "not accessible to road vehicles and thus will not be imported. "
            "A list of access tags with description is available on the "
            "[OpenStreetMap wiki](https://wiki.openstreetmap.org/wiki/Key:access)."
        ),
    )

    input_files = {"simulation_area": InputFile(SimulationAreaFile, optional=True)}
    output_files = {"raw_edges": RawEdgesFile}

    def is_defined(self) -> bool:
        return self.crs is not None and self.osm_file is not None and self.highways is not None

    def run(self):
        edges = import_osm_road_network(
            osm_file=self.osm_file,
            highways=self.highways,
            crs=self.crs,
            simulation_area_file=self.input["simulation_area"],  # ty: ignore[invalid-argument-type]
            allowed_access=self.allowed_access,
        )
        self.output["raw_edges"].write(edges)
