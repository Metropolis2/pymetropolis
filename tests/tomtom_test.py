"""Tests for the generation of random TomTom requests."""

import geopandas as gpd
import numpy as np
import pyproj
import pytest
from shapely.geometry import LineString

from pymetropolis.metro_calibration.road import tomtom
from pymetropolis.metro_calibration.road.tomtom import draw_indices_in_window, generate_random_nodes
from pymetropolis.metro_common import MetropyError

CRS = "EPSG:2154"
# Spacing between two nodes of the grid, in meters.
SPACING = 1000.0
GRID_SIZE = 20


def grid_edges() -> tuple[gpd.GeoDataFrame, dict[int, tuple[float, float]]]:
    """Returns the edges of a grid network (one horizontal edge per node, except on the last
    column which has vertical "motorway" edges) and the coordinates of each node."""
    # Somewhere in France so that the conversion to EPSG:4326 is valid.
    x0, y0 = 650_000.0, 6_860_000.0
    xy = {
        i * GRID_SIZE + j: (x0 + i * SPACING, y0 + j * SPACING)
        for i in range(GRID_SIZE)
        for j in range(GRID_SIZE)
    }
    sources, targets, edge_types, geoms = [], [], [], []
    for i in range(GRID_SIZE):
        for j in range(GRID_SIZE):
            source = i * GRID_SIZE + j
            if i < GRID_SIZE - 1:
                target = (i + 1) * GRID_SIZE + j
                edge_type = "primary"
            else:
                target = i * GRID_SIZE + (j + 1) % GRID_SIZE
                edge_type = "motorway"
            sources.append(source)
            targets.append(target)
            edge_types.append(edge_type)
            geoms.append(LineString([xy[source], xy[target]]))
    edges = gpd.GeoDataFrame(
        {"source": sources, "target": targets, "edge_type": edge_types}, geometry=geoms, crs=CRS
    )
    return edges, xy


@pytest.fixture(params=["rejection", "exact"])
def drawing_method(request, monkeypatch):
    """Runs a test with rejection sampling and with the exact listing of candidates only."""
    if request.param == "exact":
        monkeypatch.setattr(tomtom, "MAX_REJECTION_ROUNDS", 0)
    return request.param


def test_generate_random_nodes_max_distance(drawing_method):
    edges, xy = grid_edges()
    rng = np.random.default_rng(13081996)
    max_distance = 2.5 * SPACING
    nodes, coordinates = generate_random_nodes(
        edges,
        rng,
        nb_routes=200,
        nb_waypoints=3,
        excluded_edge_types=["motorway"],
        max_distance=max_distance,
        crs=CRS,
    )
    assert nodes.shape == (200, 5)
    assert coordinates.shape == (200, 5, 2)
    points = np.array([[xy[n] for n in route] for route in nodes])
    distances = np.linalg.norm(points[:, 1:] - points[:, :-1], axis=2)
    assert distances.max() <= max_distance
    # Nodes of the last column are only sources of motorway edges.
    last_column = {(GRID_SIZE - 1) * GRID_SIZE + j for j in range(GRID_SIZE)}
    assert not last_column.intersection(nodes.flatten())
    # Coordinates are the (lat, lon) of the drawn nodes.
    transformer = pyproj.Transformer.from_crs(CRS, "EPSG:4326")
    lat, lon = transformer.transform(points[:, :, 0], points[:, :, 1])
    np.testing.assert_allclose(coordinates[:, :, 0], lat)
    np.testing.assert_allclose(coordinates[:, :, 1], lon)


def test_generate_random_nodes_no_distance_window():
    edges, _ = grid_edges()
    rng = np.random.default_rng(13081996)
    nodes, coordinates = generate_random_nodes(edges, rng, nb_routes=50, nb_waypoints=0)
    assert nodes.shape == (50, 2)
    assert coordinates.shape == (50, 2, 2)
    # Coordinates are (lat, lon).
    assert np.all((coordinates[:, :, 0] > 40) & (coordinates[:, :, 0] < 52))


def consecutive_distances(nodes: np.ndarray, xy: dict[int, tuple[float, float]]) -> np.ndarray:
    points = np.array([[xy[n] for n in route] for route in nodes])
    return np.linalg.norm(points[:, 1:] - points[:, :-1], axis=2)


def test_generate_random_nodes_min_distance(drawing_method):
    edges, xy = grid_edges()
    rng = np.random.default_rng(13081996)
    min_distance = 5 * SPACING
    nodes, _ = generate_random_nodes(
        edges, rng, nb_routes=200, nb_waypoints=3, min_distance=min_distance, crs=CRS
    )
    assert nodes.shape == (200, 5)
    assert consecutive_distances(nodes, xy).min() >= min_distance


def test_generate_random_nodes_distance_window(drawing_method):
    edges, xy = grid_edges()
    rng = np.random.default_rng(13081996)
    min_distance, max_distance = 1.5 * SPACING, 3 * SPACING
    nodes, _ = generate_random_nodes(
        edges,
        rng,
        nb_routes=200,
        nb_waypoints=3,
        min_distance=min_distance,
        max_distance=max_distance,
        crs=CRS,
    )
    distances = consecutive_distances(nodes, xy)
    assert distances.min() >= min_distance
    assert distances.max() <= max_distance


@pytest.mark.parametrize(
    ("min_distance", "max_distance"),
    [
        # No two nodes of the grid are between 1.1 and 1.2 spacings apart.
        (1.1 * SPACING, 1.2 * SPACING),
        # The grid is smaller than 100 spacings.
        (100 * SPACING, None),
    ],
)
def test_generate_random_nodes_empty_window(min_distance, max_distance, drawing_method):
    edges, _ = grid_edges()
    rng = np.random.default_rng(13081996)
    with pytest.raises(MetropyError, match="should be enlarged"):
        generate_random_nodes(
            edges,
            rng,
            nb_routes=10,
            nb_waypoints=0,
            min_distance=min_distance,
            max_distance=max_distance,
            crs=CRS,
        )


def test_generate_random_nodes_min_larger_than_max():
    edges, _ = grid_edges()
    rng = np.random.default_rng(13081996)
    with pytest.raises(MetropyError, match="larger than the maximum distance"):
        generate_random_nodes(
            edges, rng, nb_routes=10, nb_waypoints=0, min_distance=2.0, max_distance=1.0, crs=CRS
        )


def test_draw_indices_min_distance_covers_all_candidates(drawing_method):
    # Points on a line, at x = 0, 1, ..., 9.
    xy = np.column_stack([np.arange(10.0), np.zeros(10)])
    rng = np.random.default_rng(13081996)
    min_distance = 3.5
    idx = draw_indices_in_window(
        xy,
        np.arange(10),
        rng,
        nb_routes=5000,
        nb_nodes=2,
        min_distance=min_distance,
        max_distance=None,
    )
    drawn_pairs = set(map(tuple, idx.tolist()))
    valid_pairs = {(o, d) for o in range(10) for d in range(10) if abs(o - d) >= min_distance}
    # All the valid pairs (and only them) are drawn.
    assert drawn_pairs == valid_pairs
