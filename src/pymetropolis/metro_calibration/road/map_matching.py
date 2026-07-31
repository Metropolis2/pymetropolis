from __future__ import annotations

import itertools
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_network.road_network.files import RoadEdgesCleanFile
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import FloatParameter

from .files import TomTomRoutesFile, TomTomRoutesMatchedFile

if TYPE_CHECKING:
    import geopandas as gpd
    import polars as pl


def load_trajectories(path: Path, radius: float) -> pl.DataFrame:
    import duckdb

    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")

    # Read the route trajectories and buffer them by the radius.
    logger.debug("Reading buffered geometries")
    df = con.sql(
        f"""
        SELECT
            tomtom_id,
            source,
            target,
            length,
            ST_AsWKB(geometry) as wkb,
            ST_AsWKB(
                ST_Simplify(
                    ST_Buffer(
                        geometry,
                        {radius},      -- distance
                        16,            -- num_triangles
                        'CAP_SQUARE',  -- cap style
                        'JOIN_ROUND',  -- join style
                        0.0            -- mitre_limit
                    ),
                    5.0                -- simplify tolerance
                )
            ) as buffered_wkb
        FROM read_parquet('{path}')
        ORDER BY tomtom_id
        """
    ).pl()

    return df


def map_matching(edges: gpd.GeoDataFrame, trajectories: pl.DataFrame, rel_length_threshold: float):
    import geopandas as gpd
    import networkx as nx
    import polars as pl
    from shapely.geometry import Point
    from tqdm import tqdm

    logger.debug("Preparing matching")
    # Find the unique nodes in the road network graph, with their Point geometries.
    nodes = (
        edges.drop_duplicates(subset=["source"], ignore_index=True)
        .rename(columns={"source": "node_id"})
        .loc[:, ["node_id", "geometry"]]
    )
    nodes.set_geometry(nodes["geometry"].map(lambda g: Point(g.coords[0])), inplace=True)
    # Create dictionaries to find edge_id from source and target and to find edge's length from
    # edge_id.
    edges_df = pl.from_pandas(edges.loc[:, ["edge_id", "source", "target", "length"]])
    edge_ids = {
        (source, target): edge_id
        for source, target, edge_id in zip(
            edges_df["source"], edges_df["target"], edges_df["edge_id"]
        )
    }
    edge_lengths = {
        edge_id: length for edge_id, length in zip(edges_df["edge_id"], edges_df["length"])
    }
    logger.debug("Finding nodes contained within the buffered geometries")
    buffered_trajectories = gpd.GeoDataFrame(
        {"tomtom_id": trajectories["tomtom_id"]},
        geometry=gpd.GeoSeries.from_wkb(trajectories["buffered_wkb"]),
    ).set_index("tomtom_id")
    node_matches = pl.DataFrame(
        nodes.sindex.query(buffered_trajectories.geometry, predicate="contains").T,
        schema=["tomtom_id", "node_id"],
    )
    node_matches = node_matches.with_columns(
        pl.col("node_id").replace_strict(
            nodes.index.values, nodes["node_id"].values, return_dtype=pl.Int64
        )
    )
    node_matches = node_matches.group_by("tomtom_id").agg("node_id")
    # Add trajectories data (source, target and length).
    node_matches = node_matches.join(trajectories.drop("wkb"), on="tomtom_id")
    # Filter out routes for which either the origin or destination node is not in the matched nodes.
    node_matches = node_matches.filter(
        pl.col("node_id").list.contains(pl.col("source")),
        pl.col("node_id").list.contains(pl.col("target")),
    )
    node_matches = node_matches.sort("tomtom_id")
    nodes = nodes.set_index("node_id")
    results = list()
    geom_trajectories = gpd.GeoDataFrame(
        {"tomtom_id": trajectories["tomtom_id"]},
        geometry=gpd.GeoSeries.from_wkb(trajectories["wkb"]),
    )
    for row in tqdm(
        node_matches.iter_rows(named=True), total=len(node_matches), desc="Matching", smoothing=0.05
    ):
        node_ids = set(row["node_id"])
        my_edges = edges_df.filter(
            pl.col("source").is_in(node_ids) & pl.col("target").is_in(node_ids)
        ).select("source", "target", "length")
        if row["source"] not in my_edges["source"] or row["target"] not in my_edges["target"]:
            # Either source or target is not in the graph.
            continue
        G = nx.DiGraph()
        G.add_weighted_edges_from(my_edges.iter_rows())
        tree = nx.bfs_tree(G, row["source"])
        if not tree.has_node(row["target"]):
            # Source and target are not connected.
            #  assert not nx.has_path(G, row["source"], row["target"])
            continue
        dists = nodes.loc[list(tree.nodes)].distance(
            geom_trajectories.loc[row["tomtom_id"], "geometry"]
        )
        _, path_nodes = nx.bidirectional_dijkstra(
            tree, row["source"], row["target"], lambda s, _t, _w: dists[s]
        )
        path_edges = [edge_ids[(s, t)] for s, t in itertools.pairwise(path_nodes)]
        tot_length = sum(edge_lengths[e] for e in path_edges)
        results.append(
            {
                "tomtom_id": row["tomtom_id"],
                "path": path_edges,
                "length": tot_length,
                "length_tomtom": row["length"],
            }
        )
    df = (
        pl.DataFrame(results)
        .with_columns(
            rel_length_diff=(pl.col("length") - pl.col("length_tomtom")) / pl.col("length_tomtom")
        )
        .filter(pl.col("rel_length_diff").abs() <= rel_length_threshold)
    )
    n = len(df)
    s = n / len(trajectories)
    logger.info(f"{n:,} routes were matched (representing {s:.1%} of routes)")
    return df


class MapMatchingStep(Step):
    """Match road trajectories to the actual road network."""

    radius = FloatParameter(
        "tomtom_requests.map_matching.search_radius",
        description="Search radius, in meters.",
        default=100.0,
        note=(
            "Larger values are more likely to result in a positive match but increase running time."
            "Recommended value is between 30 and 200."
        ),
    )
    relative_length_threshold = FloatParameter(
        "tomtom_requests.map_matching.relative_length_threshold",
        description=(
            "Maximum relative difference allowed between TomTom route's length and actual length "
            "on network."
        ),
        default=0.005,
        note=(
            "A value of 0.01 means that routes for which the length of the matched path on the "
            "road-network differs by more than 1% compared to the length of the TomTom route will "
            "be excluded."
        ),
    )

    input_files = {"edges": RoadEdgesCleanFile, "routes": TomTomRoutesFile}
    output_files = {"results": TomTomRoutesMatchedFile}
    priority = 0

    def run(self):
        assert self.radius is not None
        assert self.relative_length_threshold is not None

        edges = self.input["edges"].read()

        trajectories = load_trajectories(self.input["routes"].complete_path, self.radius)

        df = map_matching(edges, trajectories, self.relative_length_threshold)

        self.output["results"].write(df)
