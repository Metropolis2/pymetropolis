"""Tests for the reading of urban areas from OpenStreetMap data.

The tests run on a small synthetic OSM file with the following areas:

- Closed way 10 (`landuse=residential`): square of 0.01° x 0.01°.
- Relation 100 (`landuse=industrial`): square of 0.01° x 0.01° (way 11), with two overlapping
  inner rings (ways 12 and 13, which is not a valid OSM multipolygon).
- Closed way 14 (`landuse=forest`): not urban.
- Closed way 15 (`landuse=retail`): far away from the other areas.
"""

from pathlib import Path

import pyproj
import pytest
from shapely.geometry import box

from pymetropolis.metro_spatial.osm import read_osm_areas
from pymetropolis.metro_spatial.urban_areas.osm import read_osm_urban_areas

URBAN = ["residential", "industrial", "retail"]
CRS = pyproj.CRS("EPSG:2154")


def square(x0: float, y0: float, size: float) -> list[tuple[float, float]]:
    return [(x0, y0), (x0 + size, y0), (x0 + size, y0 + size), (x0, y0 + size)]


@pytest.fixture
def osm_file(tmp_path: Path) -> Path:
    import osmium
    from osmium.osm.mutable import Node, Relation, Way

    rings = {
        10: (square(3.00, 45.00, 0.01), {"landuse": "residential"}),
        11: (square(3.02, 45.00, 0.01), {}),
        12: (square(3.022, 45.002, 0.004), {}),
        13: (square(3.024, 45.004, 0.004), {}),
        14: (square(3.04, 45.00, 0.01), {"landuse": "forest"}),
        15: (square(4.00, 46.00, 0.01), {"landuse": "retail"}),
    }
    path = tmp_path / "test.osm.pbf"
    with osmium.SimpleWriter(str(path)) as writer:
        node_id = 0
        ways = list()
        for way_id, (coords, tags) in rings.items():
            node_ids = list()
            for coord in coords:
                node_id += 1
                writer.add_node(Node(id=node_id, location=coord))
                node_ids.append(node_id)
            ways.append(Way(id=way_id, nodes=[*node_ids, node_ids[0]], tags=tags))
        for way in ways:
            writer.add_way(way)
        writer.add_relation(
            Relation(
                id=100,
                members=[("w", 11, "outer"), ("w", 12, "inner"), ("w", 13, "inner")],
                tags={"type": "multipolygon", "landuse": "industrial"},
            )
        )
    return path


class SimulationArea:
    def __init__(self, area):
        self.area = area

    def get_area_opt(self):
        return self.area


def test_read_osm_areas(osm_file: Path):
    areas = read_osm_areas(
        osm_file,
        "list_contains($landuse, tags['landuse'])",
        {"landuse": URBAN},
        {"landuse": "tags['landuse']"},
    ).set_index(["kind", "osm_id"])
    assert set(areas.index) == {("way", 10), ("relation", 100), ("way", 15)}
    assert areas.loc[("relation", 100), "landuse"] == "industrial"
    assert areas.geometry.is_valid.all()
    assert areas.geometry[("way", 10)].area == pytest.approx(0.01**2)
    # The overlapping inner rings are made valid (their intersection is not a hole).
    industrial = areas.geometry[("relation", 100)]
    assert industrial.area == pytest.approx(0.01**2 - 2 * 0.004**2 + 2 * 0.002**2)


def test_read_osm_urban_areas(osm_file: Path):
    gdf = read_osm_urban_areas(osm_file, URBAN, 0.0, CRS, SimulationArea(None))  # ty: ignore[invalid-argument-type]
    assert len(gdf) == 1
    assert gdf.crs == CRS
    assert gdf.geometry[0].is_valid
    # Residential, retail and industrial areas, with the intersection of the inner rings of the
    # industrial area as an island.
    assert len(gdf.geometry[0].geoms) == 4


def test_read_osm_urban_areas_with_filter(osm_file: Path):
    transformer = pyproj.Transformer.from_crs("EPSG:4326", CRS, always_xy=True)
    x0, y0 = transformer.transform(2.99, 44.99)
    x1, y1 = transformer.transform(3.05, 45.02)
    area = box(x0, y0, x1, y1)
    gdf = read_osm_urban_areas(osm_file, URBAN, 0.0, CRS, SimulationArea(area))  # ty: ignore[invalid-argument-type]
    # The retail area is outside the simulation area.
    assert len(gdf.geometry[0].geoms) == 3
