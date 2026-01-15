import os

import geopandas as gpd
import numpy as np
import osmium
import polars as pl
from loguru import logger
from osmium import IdTracker
from osmium.filter import EntityFilter, TagFilter
from osmium.geom import WKBFactory
from osmium.osm import NODE, WAY
from shapely.geometry import LineString
from shapely.prepared import prep

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_pipeline import Config, ConfigTable, ConfigValue, Step
from pymetropolis.metro_spatial import CRS, OSM_FILE
from pymetropolis.metro_spatial.simulation_area import SIMULATION_AREA_FILE

from .files import RAW_EDGES_FILE

HIGHWAYS = ConfigValue(
    "osm_road_import.highways",
    key="highways",
    expected_type=list[str],
    description="List of `highway=*` OpenStreetMap tags to be considered as valid road ways.",
    example='`["motorway", "motorway_link", "trunk", "trunk_link", "primary", "primary_link"]`',
    note="For a list of highway tags with description, see the OpenStreetMap wiki.",
)

URBAN_LANDUSE = ConfigValue(
    "osm_road_import.urban_landuse",
    key="urban_landuse",
    expected_type=list[str],
    default=list(),
    description="List of `landuse=*` OpenStreetMap tags that define an urban area.",
    example='`["residential", "industrial", "commercial", "retail"]`',
    note=(
        "For a list of landuse tags with description, see the OpenStreetMap wiki. The urban areas "
        "are used to defined an urban vs rural flag on roads, which is in turn used to define "
        "default values (e.g., default speed limit of 50 km/h on urban roads vs 80 km/h on rural "
        "roads)."
    ),
)

URBAN_BUFFER = ConfigValue(
    "osm_road_import.urban_buffer",
    key="urban_buffer",
    expected_type=float,
    default=0.0,
    description="Distance by which the polygons of the urban areas will be buffered.",
    note=(
        "The value is expressed in the unit of measure of the CRS (usually meter). Positive values "
        "extend the area, while negative values shrink it. A road edge must be entirely contained "
        "within urban areas to be classified as a urban edge so it is recommended to use a "
        "positive value to correctly identify urban edges."
    ),
)

OSM_ROAD_IMPORT_CONFIGS = [HIGHWAYS, URBAN_LANDUSE, URBAN_BUFFER]

OSM_ROAD_IMPORT_TABLE = ConfigTable(
    "osm_road_import",
    "osm_road_import",
    items=OSM_ROAD_IMPORT_CONFIGS,
    description="Extract the road network, as a graph of directed edges, from OpenStreetMap data.",
)

# Dictionary for special `maxspeed` values.
SPEED_DICT = {
    "walk": 8,
    "FR:walk": 20,
    "FR:urban": 50,
    "FR:rural": 80,
}

# Conversion miles to kilometers.
M_TO_KM = 1.609344


def import_osm(config: Config):
    """Main function to import a road network from OpenStreetMap data."""
    input_filename = config[OSM_FILE]

    if SIMULATION_AREA_FILE.exists(config):
        # TODO: Read properly
        filter_polygon = SIMULATION_AREA_FILE.read(config)
    else:
        filter_polygon = None
    filter_polygon = get_simulation_area(config)

    way_id_tracker = filter_highway_ways(
        input_filename, config[CRS], config[HIGHWAYS], filter_polygon
    )

    edges, node_id_tracker = read_highway_ways(input_filename, way_id_tracker)

    nodes = read_highway_nodes(input_filename, node_id_tracker)

    edges_gdf = create_edges(edges, nodes, config[CRS])

    if not config[URBAN_LANDUSE]:
        urban_area = None
    else:
        urban_area, gdf = read_urban_area(
            input_filename, config[CRS], config[URBAN_LANDUSE], config[URBAN_BUFFER], filter_polygon
        )
        URBAN_AREAS_FILE.save(gdf)

    edges_gdf = add_urban_tag(edges_gdf, urban_area)

    logger.info("Saving road-network edges")
    RAW_EDGES_FILE.save(edges_gdf)

    save_stats(edges_gdf, config.path("osm_road_import.stats"))

    plot_variables(edges_gdf, config.path("osm_road_import.graphs"))


def save_stats(gdf: gpd.GeoDataFrame, output_filename: str):
    """Computes and writes some stats on the imported road network to a file."""
    logger.info(f"Saving output statistics to `{output_filename}`")
    stats = ""
    nb_nodes = len(set(gdf["source"]).union(set(gdf["target"])))
    stats += f"Number of nodes: {nb_nodes:,}\n"
    nb_edges = len(gdf)
    stats += f"Number of edges: {nb_edges:,}\n"
    nb_urbans = gdf["urban"].sum()
    stats += f"Number of urban edges: {nb_urbans:,} ({nb_urbans / nb_edges:.1%})\n"
    nb_rurals = nb_edges - nb_urbans
    stats += f"Number of rural edges: {nb_rurals:,} ({nb_rurals / nb_edges:.1%})\n"
    nb_roundabouts = gdf["roundabout"].sum()
    stats += f"Number of roundabout edges: {nb_roundabouts:,} ({nb_roundabouts / nb_edges:.1%})\n"
    nb_traffic_signals = gdf["traffic_signals"].sum()
    stats += f"Number of edges with traffic signals: {nb_traffic_signals:,} ({nb_traffic_signals / nb_edges:.1%})\n"
    nb_stop_signs = gdf["stop"].sum()
    stats += f"Number of edges with stop sign: {nb_stop_signs:,} ({nb_stop_signs / nb_edges:.1%})\n"
    nb_give_way_signs = gdf["give_way"].sum()
    stats += f"Number of edges with give_way sign: {nb_give_way_signs:,} ({nb_give_way_signs / nb_edges:.1%})\n"
    nb_tolls = gdf["toll"].sum()
    stats += f"Number of edges with toll: {nb_tolls:,} ({nb_tolls / nb_edges:.1%})\n"
    tot_length = gdf["length"].sum() / 1e3
    stats += f"Total edge length (km): {tot_length:,.3f}\n"
    urban_length = gdf.loc[gdf["urban"], "length"].sum() / 1e3
    stats += (
        f"Total urban edge length (km): {urban_length:,.3f} ({urban_length / tot_length:.1%})\n"
    )
    rural_length = tot_length - urban_length
    stats += (
        f"Total rural edge length (km): {rural_length:,.3f} ({rural_length / tot_length:.1%})\n"
    )
    speed_na_count = gdf["speed_limit"].isna().sum()
    stats += f"Number of null values for speed_limit: {speed_na_count:,} ({speed_na_count / nb_edges:.1%})\n"
    lanes_na_count = gdf["lanes"].isna().sum()
    stats += (
        f"Number of null values for lanes: {lanes_na_count:,} ({lanes_na_count / nb_edges:.1%})\n"
    )
    io.write_stats(stats, output_filename)


def plot_variables(gdf: gpd.GeoDataFrame, graph_dir: str):
    """Plots and saves some graphs of the main variables."""
    logger.info(f"Generating graphs of the variables to {graph_dir}")
    if not os.path.isdir(graph_dir):
        os.makedirs(graph_dir)
    # Length distribution hist.
    fig, ax = plots.get_figure(fraction=0.8)
    bins = np.logspace(np.log(gdf["length"].min()), np.log(gdf["length"].max()), 50, base=np.e)
    ax.hist(gdf["length"], bins=list(bins), density=True, color=plots.CMP(0))
    ax.set_xscale("log")
    ax.set_xlabel("Length (meters, log scale)")
    ax.set_ylabel("Density")
    fig.tight_layout()
    fig.savefig(os.path.join(graph_dir, "length_distribution.pdf"))
    # Speed limit distribution bar plot.
    fig, ax = plots.get_figure(fraction=0.8)
    bins = np.arange(
        np.floor(gdf["speed_limit"].min() / 5.0) * 5.0 - 2.5,
        np.ceil(gdf["speed_limit"].max() / 5.0) * 5.0 + 2.5 + 1.0,
        5.0,
    )
    ax.hist(gdf["speed_limit"], bins=bins, density=True, color=plots.CMP(0))
    ax.set_xlabel("Speed limit (km/h)")
    ax.set_ylabel("Density")
    fig.tight_layout()
    fig.savefig(os.path.join(graph_dir, "speed_limit_distribution.pdf"))
    # Speed limit distribution bar plot, weighted by length.
    fig, ax = plots.get_figure(fraction=0.8)
    bins = np.arange(
        np.floor(gdf["speed_limit"].min() / 5.0) * 5.0 - 2.5,
        np.ceil(gdf["speed_limit"].max() / 5.0) * 5.0 + 2.5 + 1.0,
        5.0,
    )
    ax.hist(gdf["speed_limit"], bins=bins, density=True, weights=gdf["length"], color=plots.CMP(0))
    ax.set_xlabel("Speed limit (km/h)")
    ax.set_ylabel("Density (weighted by edge length)")
    fig.tight_layout()
    fig.savefig(os.path.join(graph_dir, "speed_limit_distribution_length_weights.pdf"))
    # Lanes distribution bar plot.
    fig, ax = plots.get_figure(fraction=0.8)
    mask = ~gdf["lanes"].isna()
    bins = [0.5, 1.5, 2.5, 3.5, gdf["lanes"].max()]
    bars, _ = np.histogram(gdf.loc[mask, "lanes"], bins=bins)
    bars = bars / mask.sum()
    xs = np.arange(1, 5)
    ax.bar(xs, bars, width=1.0, color=plots.CMP(0))
    ax.set_xlabel("Number of lanes")
    ax.set_xticks(xs, ["1", "2", "3", "4+"])
    ax.set_ylabel("Density")
    fig.tight_layout()
    fig.savefig(os.path.join(graph_dir, "lanes_distribution.pdf"))
    # Lanes distribution bar plot, weighted by length.
    fig, ax = plots.get_figure(fraction=0.8)
    bins = [0.5, 1.5, 2.5, 3.5, gdf["lanes"].max()]
    bars, _ = np.histogram(gdf.loc[mask, "lanes"], bins=bins, weights=gdf.loc[mask, "length"])
    bars = bars / gdf.loc[mask, "length"].sum()
    xs = np.arange(1, 5)
    ax.bar(xs, bars, width=1.0, color=plots.CMP(0))
    ax.set_xlabel("Number of lanes")
    ax.set_xticks(xs, ["1", "2", "3", "4+"])
    ax.set_ylabel("Density (weighted by edge length)")
    fig.tight_layout()
    fig.savefig(os.path.join(graph_dir, "lanes_distribution_length_weights.pdf"))
    # Road type chart.
    fig, ax = plots.get_figure(fraction=0.6, ratio=1)
    road_type_lengths = gdf["road_type"].value_counts().sort_index()
    # Set the road types with a share <= 2 % in a dedicated category.
    pct_threshold = 2 * len(gdf) / 100
    lengths = dict()
    if (road_type_lengths <= pct_threshold).any():
        lengths["other"] = 0
    for key, value in road_type_lengths.to_dict().items():
        if value <= pct_threshold:
            lengths["other"] += value
        else:
            lengths[key] = value
    ax.pie(
        list(lengths.values()),
        labels=list(lengths.keys()),
        autopct=lambda p: f"{p:.1f}\\%",
        pctdistance=0.75,
        labeldistance=1.05,
        colors=plots.COLOR_LIST,
    )
    fig.tight_layout()
    fig.savefig(os.path.join(graph_dir, "road_type_pie.pdf"))
    # Road type chart, weighted by length.
    fig, ax = plots.get_figure(fraction=0.6, ratio=1)
    road_type_lengths = gdf.groupby("road_type")["length"].sum().sort_index()
    # Set the road types with a share <= 2 % in a dedicated category.
    pct_threshold = 2 * gdf["length"].sum() / 100
    lengths = dict()
    if (road_type_lengths <= pct_threshold).any():
        lengths["other"] = 0
    for key, value in road_type_lengths.to_dict().items():
        if value <= pct_threshold:
            lengths["other"] += value
        else:
            lengths[key] = value
    ax.pie(
        list(lengths.values()),
        labels=list(lengths.keys()),
        autopct=lambda p: f"{p:.1f}\\%",
        pctdistance=0.75,
        labeldistance=1.05,
        colors=plots.COLOR_LIST,
    )
    fig.tight_layout()
    fig.savefig(os.path.join(graph_dir, "road_type_pie_length_weights.pdf"))


def filter_highway_ways(
    osm_filename: str, crs, highways: list[str], filter_polygon=None
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
        if not is_valid_way(way):
            continue
        ids.append(way.id)
        linestrings.append(fab.create_linestring(way.nodes))
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
            msg = "The simulation area does not intersect with the OSM data"
            raise MetropyError(msg)
    logger.debug("Creating id tracker")
    tracker = IdTracker()
    for way_id in gdf["id"].values:
        tracker.add_way(way_id)
    return tracker


def is_valid_way(way):
    has_access = "access" not in way.tags or way.tags["access"] in (
        "yes",
        "permissive",
        "destination",
    )
    return has_access and len(way.nodes) >= 2 and not way.is_closed()


def read_highway_ways(
    osm_filename: str, way_id_tracker: IdTracker
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
                "nodes": tuple(n.ref for n in way.nodes),
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
    df = lf.select(
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
    ).collect()
    return df, node_id_tracker


def read_highway_nodes(osm_filename: str, node_id_tracker: IdTracker) -> pl.DataFrame:
    """Reads all the nodes in the OSM file with a valid id and returns a DataFrame with their
    characteristics (including coordinates)."""
    logger.info("Reading highway nodes")
    data = list()
    logger.debug("Reading nodes from OSM file")
    for node in osmium.FileProcessor(osm_filename, NODE).with_filter(node_id_tracker.id_filter()):
        data.append(
            {
                "osm_id": node.id,
                "lat": node.lat,
                "lon": node.lon,
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
    forward_edges = lf.with_columns(*(pl.col(f"forward_{feat}").alias(feat) for feat in features))
    backward_edges = lf.filter(pl.col("oneway").not_()).with_columns(
        pl.col("source").alias("target"),
        pl.col("target").alias("source"),
        pl.col("nodes").list.reverse(),
        *(pl.col(f"backward_{feat}").alias(feat) for feat in features),
    )
    lf = pl.concat((forward_edges, backward_edges), how="vertical")
    # Sort by source then value to make geographically close edges next to each other.
    lf = lf.sort("source", "target")
    # Create id column.
    lf = lf.with_columns(pl.int_range(pl.len()).alias("edge_id"))
    edges = lf.select(
        "edge_id",
        "osm_id",
        "source",
        "target",
        "road_type",
        "name",
        "toll",
        "roundabout",
        "oneway",
        *features,
        "nodes",
    ).collect()
    logger.debug("Creating edge geometries")
    geoms = [
        LineString(coords)
        for coords in edges["nodes"].list.eval(
            pl.element().replace_strict(nodes["osm_id"], nodes["coords"])
        )
    ]
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

    Edges has marked as having the feature is in the forward direction if:
    - One of the edge's nodes is marked as having the feature in forward direction.
    - The target node of the edge is marked as having the feature (with no direction specified).
    - The edge is "oneway" and one of its nodes is marked as having the feature (with no direction
      specified).

    Edges has marked as having the feature is in the backward direction if:
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


def read_urban_area(
    osm_filename: str,
    crs,
    urban_tags: list[str],
    buffer: float,
    filter_polygon=None,
    output_filename: str | None = None,
):
    """Reads the areas with a urban landuse in the OSM file and returns a MultiPolygon representing
    all these urban areas.
    """
    logger.info("Reading urban areas")
    ids = list()
    tags = list()
    polygons = list()
    fab = WKBFactory()
    valid_tag_pairs = tuple(("landuse", tag) for tag in urban_tags)
    logger.debug("Reading areas from OSM file")
    for area in (
        osmium.FileProcessor(osm_filename).with_filter(TagFilter(*valid_tag_pairs)).with_areas()
    ):
        if area.is_area():
            ids.append(area.id)
            tags.append(area.tags["landuse"])
            polygons.append(fab.create_multipolygon(area))
    logger.debug("Building GeoDataFrame")
    gdf = gpd.GeoDataFrame(
        {"osm_id": ids, "landuse": tags}, geometry=gpd.GeoSeries.from_wkb(polygons, crs="EPSG:4326")
    )
    logger.debug("Converting to required CRS")
    gdf.to_crs(crs, inplace=True)
    if filter_polygon is not None:
        logger.debug("Filtering based on area")
        mask = [filter_polygon.intersects(geom) for geom in gdf.geometry]
        gdf = gdf.loc[mask].copy()
    logger.debug("Computing union of all urban areas")
    urban_area = gdf.union_all()
    logger.debug("Buffering and simplifying geometry")
    urban_area = urban_area.buffer(buffer).simplify(0, preserve_topology=False)
    return prep(urban_area), gdf


def add_urban_tag(edges_gdf: gpd.GeoDataFrame, urban_area):
    """Adds a column to the edges GeoDataFrame representing whether the edge is within a urban
    area."""
    logger.debug("Computing edges' urban tag")
    if urban_area is None:
        edges_gdf["urban"] = False
    else:
        edges_gdf["urban"] = [urban_area.contains(geom) for geom in edges_gdf.geometry]
    return edges_gdf


OSM_ROAD_IMPORT = Step(
    "osm-road-import",
    import_osm,
    output_files=[RAW_EDGES_FILE],
    config_values=OSM_ROAD_IMPORT_CONFIGS + [CRS, OSM_FILE],
)
