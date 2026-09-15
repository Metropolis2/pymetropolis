from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    import geopandas as gpd


def find_kmedoids(
    edges: gpd.GeoDataFrame,
    zones: gpd.GeoDataFrame,
    nb_clusters: int,
    random_seed: int | None = None,
) -> gpd.GeoDataFrame:
    """Identifiers representative points in zones from the K-Medoids algorithm.

    edges: GeoDataFrame of edges of the network (source, length, geometry).
    zones: GeoDataFrame of zones (zone_id, geometry).
    nb_clusters: Number of representative points computed.
    random_seed: Random seed of the K-Medoids algorithm.
    """
    from itertools import cycle, islice

    import geopandas as gpd
    import numpy as np
    import polars as pl
    from shapely.geometry import Point
    from sklearn_extra.cluster import KMedoids

    nodes = gpd.GeoDataFrame(
        edges.groupby("source").agg({"length": "sum", "geometry": "first"}), crs=edges.crs
    )
    nodes = nodes.reset_index(names="node_id")
    nodes["geometry"] = nodes["geometry"].apply(lambda geom: Point(geom.coords[0]))
    nodes = nodes.sjoin(zones.to_crs(nodes.crs), predicate="intersects", how="inner")
    nodes["x"] = nodes.geometry.x
    nodes["y"] = nodes.geometry.y
    nodes_df = pl.from_pandas(nodes.drop(columns=["geometry", "index_right"]))

    medoids = list()
    for (zone_id,), zone_nodes in nodes_df.partition_by(
        "zone_id", include_key=False, as_dict=True
    ).items():
        X = zone_nodes.select("x", "y").to_numpy()
        if len(X) < nb_clusters:
            # There are fewer nodes than clusters: return each node as a center.
            # Cycle the nodes so that the number of centers stay fixed.
            centers = np.fromiter(islice(cycle(X), nb_clusters), dtype=np.dtype((float, 2)))
            weights = np.repeat(1 / len(centers), len(centers))
        else:
            kmedoids = KMedoids(n_clusters=nb_clusters, random_state=random_seed).fit(X)
            centers = kmedoids.cluster_centers_
            values_by_cluster = (
                zone_nodes.with_columns(cluster=pl.Series(kmedoids.labels_))
                .group_by("cluster")
                .agg(pl.col("length").sum())
                .with_columns(weight=pl.col("length") / pl.col("length").sum())
                .sort("cluster")
            )
            weights = values_by_cluster["weight"].to_numpy()
        medoids.extend(
            [
                [zone_id, i + 1, weights[i], centers[i][0], centers[i][1]]
                for i in range(len(centers))
            ]
        )
    medoids_df = pl.DataFrame(
        medoids, schema=["zone_id", "index", "weight", "x", "y"], orient="row"
    )
    n = medoids_df["zone_id"].n_unique()
    if n < len(zones):
        logger.warning(f"No node in zone for {len(zones) - n} zones.")
    return gpd.GeoDataFrame(
        data=medoids_df.drop("x", "y").to_pandas(),
        geometry=gpd.GeoSeries.from_xy(medoids_df["x"], medoids_df["y"], crs=edges.crs),
    )
