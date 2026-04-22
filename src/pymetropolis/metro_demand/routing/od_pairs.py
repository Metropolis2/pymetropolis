import geopandas as gpd
import polars as pl
from loguru import logger
from shapely.geometry import Point


def identify_od_pairs(
    edges: gpd.GeoDataFrame, origins_gdf: gpd.GeoDataFrame, destinations_gdf: gpd.GeoDataFrame
) -> pl.DataFrame:
    """Identify the origin and destination network node from origin / destination coordinates."""
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
