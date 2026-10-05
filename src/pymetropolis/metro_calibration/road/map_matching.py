from __future__ import annotations

import math
from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_network.road_network.files import RoadEdgesCleanFile
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import FloatParameter

from .files import TomTomRoutesFile, TomTomRoutesMatchedFile

if TYPE_CHECKING:
    from pathlib import Path

    import geopandas as gpd
    import numpy as np
    import polars as pl
    import shapely


def load_trajectories(path: Path, radius: float) -> pl.DataFrame:
    import duckdb

    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")

    # Read the route trajectories and buffer them by the radius.
    # The trajectories are simplified before being buffered (instead of simplifying the buffered
    # polygons), which is much faster and as accurate.
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
                ST_Buffer(
                    ST_Simplify(
                        geometry,
                        5.0            -- simplify tolerance
                    ),
                    {radius},          -- distance
                    16,                -- num_triangles
                    'CAP_SQUARE',      -- cap style
                    'JOIN_ROUND',      -- join style
                    0.0                -- mitre_limit
                )
            ) as buffered_wkb
        FROM read_parquet('{path}')
        ORDER BY tomtom_id
        """
    ).pl()

    return df


def trajectory_distances(points: np.ndarray, trajectory: shapely.Geometry) -> np.ndarray:
    """Returns the distance between each point and the trajectory.

    The trajectory is split into segments indexed in a STRtree, which is much faster than
    `shapely.distance` (that scans all the trajectory's segments for each point) when the
    trajectory has many vertices and the points are close to it.
    """
    import numpy as np
    import shapely

    coords = shapely.get_coordinates(trajectory)
    segments = shapely.linestrings(np.stack([coords[:-1], coords[1:]], axis=1))
    (point_idx, _), dists = shapely.STRtree(segments).query_nearest(
        points, return_distance=True, all_matches=False
    )
    out = np.empty(len(points))
    out[point_idx] = dists
    return out


def shortest_path(
    adjacency: dict[int, list[tuple[int, int]]],
    allowed: set[int],
    costs: dict[int, float],
    source: int,
    target: int,
) -> list[int] | None:
    """Dijkstra's algorithm restricted to the `allowed` nodes, where the cost of an edge is the cost
    of its source node.

    Returns the list of edge ids of the shortest path from `source` to `target`, or `None` if
    `target` cannot be reached from `source`.
    """
    import heapq

    best = {source: 0.0}
    # For each reached node, the (previous node, edge id) used to reach it.
    pred: dict[int, tuple[int, int]] = dict()
    settled = set()
    heap = [(0.0, source)]
    while heap:
        d, u = heapq.heappop(heap)
        if u in settled:
            continue
        if u == target:
            path = list()
            while u != source:
                u, edge_id = pred[u]
                path.append(edge_id)
            path.reverse()
            return path
        settled.add(u)
        new_d = d + costs[u]
        for v, edge_id in adjacency.get(u, ()):
            if v in allowed and new_d < best.get(v, math.inf):
                best[v] = new_d
                pred[v] = (u, edge_id)
                heapq.heappush(heap, (new_d, v))
    return None


def map_matching(edges: gpd.GeoDataFrame, trajectories: pl.DataFrame, rel_length_threshold: float):
    import numpy as np
    import polars as pl
    import shapely
    from tqdm import tqdm

    logger.debug("Preparing matching")
    # Find the unique nodes in the road network graph, with their Point geometries.
    node_ids, first_pos = np.unique(edges["source"].to_numpy(), return_index=True)
    node_points = shapely.get_point(edges.geometry.values[first_pos], 0)
    node_tree = shapely.STRtree(node_points)
    # Adjacency lists: source -> [(target, edge_id)]. Parallel edges are deduplicated (the last one
    # is kept).
    edge_ids = {
        (s, t): e
        for s, t, e in zip(
            edges["source"].to_numpy().tolist(),
            edges["target"].to_numpy().tolist(),
            edges["edge_id"].to_numpy().tolist(),
        )
    }
    adjacency: dict[int, list[tuple[int, int]]] = dict()
    for (s, t), e in edge_ids.items():
        adjacency.setdefault(s, []).append((t, e))
    edge_lengths = dict(
        zip(edges["edge_id"].to_numpy().tolist(), edges["length"].to_numpy().tolist())
    )

    logger.debug("Finding nodes contained within the buffered geometries")
    buffered = shapely.from_wkb(trajectories["buffered_wkb"].to_numpy())
    traj_idx, node_idx = node_tree.query(buffered, predicate="contains")
    # `query` returns pairs sorted by trajectory index: split the matched nodes by trajectory.
    split_at = np.flatnonzero(np.diff(traj_idx)) + 1
    matched_trajs = traj_idx[np.r_[0, split_at]] if len(traj_idx) else traj_idx
    matched_nodes = np.split(node_idx, split_at) if len(node_idx) else []

    lines = shapely.from_wkb(trajectories["wkb"].to_numpy())
    tomtom_ids = trajectories["tomtom_id"].to_list()
    sources = trajectories["source"].to_list()
    targets = trajectories["target"].to_list()
    lengths = trajectories["length"].to_list()
    results = {"tomtom_id": [], "path": [], "length": [], "length_tomtom": []}
    for i, nodes_idx in tqdm(
        zip(matched_trajs.tolist(), matched_nodes),
        total=len(matched_nodes),
        desc="Matching",
        smoothing=0.05,
    ):
        allowed_list = node_ids[nodes_idx].tolist()
        allowed = set(allowed_list)
        source, target = sources[i], targets[i]
        if source not in allowed or target not in allowed:
            # Either the origin or destination node is not within the buffered geometry.
            continue
        # The cost of an edge is the distance between its source node and the trajectory.
        costs = dict(
            zip(allowed_list, trajectory_distances(node_points[nodes_idx], lines[i]).tolist())
        )
        path = shortest_path(adjacency, allowed, costs, source, target)
        if path is None:
            # Source and target are not connected.
            continue
        results["tomtom_id"].append(tomtom_ids[i])
        results["path"].append(path)
        results["length"].append(sum(edge_lengths[e] for e in path))
        results["length_tomtom"].append(lengths[i])
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
