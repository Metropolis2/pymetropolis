"""Tests for the reading of the TomTom trajectories used for map matching."""

from pathlib import Path

import geopandas as gpd
import pytest
import shapely
from shapely.geometry import LineString

from pymetropolis.metro_calibration.road.map_matching import load_trajectories

RADIUS = 15.0
# The middle point is within the simplify tolerance of the straight line.
ALMOST_STRAIGHT = LineString([(0, 0), (100, 4), (200, 0)])
STRAIGHT = LineString([(0, 100), (50, 100)])


@pytest.fixture
def trajectories_file(tmp_path: Path) -> Path:
    path = tmp_path / "trajectories.parquet"
    gpd.GeoDataFrame(
        {"tomtom_id": [2, 1], "source": [20, 10], "target": [21, 11], "length": [50.0, 200.0]},
        geometry=[STRAIGHT, ALMOST_STRAIGHT],
        crs="EPSG:2154",
    ).to_parquet(path)
    return path


def test_load_trajectories(trajectories_file: Path):
    df = load_trajectories(trajectories_file, RADIUS)
    assert df.columns == ["tomtom_id", "source", "target", "length", "wkb", "buffered_wkb"]
    assert df["tomtom_id"].to_list() == [1, 2]
    assert df["source"].to_list() == [10, 20]
    assert df["target"].to_list() == [11, 21]
    assert df["length"].to_list() == [200.0, 50.0]


def test_load_trajectories_keeps_original_geometry(trajectories_file: Path):
    df = load_trajectories(trajectories_file, RADIUS)
    lines = shapely.from_wkb(df["wkb"].to_list())
    assert lines[0].equals(ALMOST_STRAIGHT)
    assert lines[1].equals(STRAIGHT)


def test_load_trajectories_buffers_simplified_geometry(trajectories_file: Path):
    df = load_trajectories(trajectories_file, RADIUS)
    buffers = shapely.from_wkb(df["buffered_wkb"].to_list())
    # With square caps, the buffer of a straight line is a rectangle extending beyond both ends.
    width = 2 * RADIUS
    assert buffers[0].area == pytest.approx((200 + width) * width)
    assert buffers[1].area == pytest.approx((50 + width) * width)
    assert buffers[1].bounds == pytest.approx((-RADIUS, 85.0, 50 + RADIUS, 115.0))
