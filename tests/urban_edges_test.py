"""Tests for the classification of road edges within urban areas."""

import geopandas as gpd
from shapely.geometry import LineString, MultiPolygon, Polygon, box

from pymetropolis.metro_network.road_network.urban import add_urban_tag

CRS = "EPSG:2154"


def urban_areas(geometry) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(geometry=[geometry], crs=CRS)


def edges(lines: dict[int, LineString]) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame({"edge_id": list(lines.keys())}, geometry=list(lines.values()), crs=CRS)


def test_add_urban_tag():
    # Square with a hole, plus a second square on its right (separated by a gap).
    with_hole = Polygon(box(0, 0, 100, 100).exterior.coords, [box(40, 40, 60, 60).exterior.coords])
    area = urban_areas(MultiPolygon([with_hole, box(110, 0, 200, 100)]))
    lines = {
        # Edge ids are not sorted to check that the order is preserved.
        5: LineString([(10, 10), (30, 10)]),  # Inside the first square.
        3: LineString([(150, 50), (160, 60), (170, 50)]),  # Inside the second square.
        9: LineString([(90, 10), (150, 10)]),  # Crossing the boundary.
        1: LineString([(300, 300), (310, 310)]),  # Outside.
        7: LineString([(45, 45), (55, 55)]),  # Inside the hole.
        2: LineString([(10, 30), (50, 30), (50, 50)]),  # Entering the hole.
        4: LineString([(95, 50), (115, 50)]),  # Spanning the two squares (through the gap).
    }
    df = add_urban_tag(edges(lines), area)
    assert df["edge_id"].to_list() == list(lines.keys())
    assert df["urban"].to_list() == [True, True, False, False, False, False, False]


def test_add_urban_tag_empty_area():
    df = add_urban_tag(edges({1: LineString([(0, 0), (1, 1)])}), urban_areas(Polygon()))
    assert df["urban"].to_list() == [False]
