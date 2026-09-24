"""Tests for the reading of administrative boundaries from OpenStreetMap data.

The tests run on a small synthetic OSM file with the following areas (`admin_level=8` unless
specified otherwise):

- Relation 100 (`A`): outer ring made of two ways (10: 1-2-3, 11: 3-4-1), with an inner ring (way
  12: 5-6-7-8-5).
- Closed way 13 (`B`): 9-10-11-12-9.
- Relation 101 (`C`): references a way which is missing from the data.
- Relation 102 (`D`): same geometry as `A` but with `admin_level=6`.
"""

from pathlib import Path

import pytest

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_pipeline import Config
from pymetropolis.metro_spatial.simulation_area.osm import (
    SimulationAreaFromOSMStep,
    read_admin_areas,
)

NODES = {
    1: (3.00, 45.00),
    2: (3.01, 45.00),
    3: (3.01, 45.01),
    4: (3.00, 45.01),
    5: (3.004, 45.004),
    6: (3.006, 45.004),
    7: (3.006, 45.006),
    8: (3.004, 45.006),
    9: (3.02, 45.00),
    10: (3.03, 45.00),
    11: (3.03, 45.01),
    12: (3.02, 45.01),
}

BOUNDARY = {"boundary": "administrative", "type": "boundary"}


@pytest.fixture
def osm_file(tmp_path: Path) -> Path:
    import osmium
    from osmium.osm.mutable import Node, Relation, Way

    path = tmp_path / "test.osm.pbf"
    with osmium.SimpleWriter(str(path)) as writer:
        for node_id, location in NODES.items():
            writer.add_node(Node(id=node_id, location=location))
        writer.add_way(Way(id=10, nodes=[1, 2, 3]))
        writer.add_way(Way(id=11, nodes=[3, 4, 1]))
        writer.add_way(Way(id=12, nodes=[5, 6, 7, 8, 5]))
        writer.add_way(
            Way(
                id=13,
                nodes=[9, 10, 11, 12, 9],
                tags={"boundary": "administrative", "admin_level": "8", "name": "B"},
            )
        )
        members = [("w", 10, "outer"), ("w", 11, "outer"), ("w", 12, "inner"), ("n", 5, "label")]
        writer.add_relation(
            Relation(id=100, members=members, tags={**BOUNDARY, "admin_level": "8", "name": "A"})
        )
        writer.add_relation(
            Relation(
                id=101,
                members=[("w", 10, "outer"), ("w", 999, "outer")],
                tags={**BOUNDARY, "admin_level": "8", "name": "C"},
            )
        )
        writer.add_relation(
            Relation(id=102, members=members, tags={**BOUNDARY, "admin_level": "6", "name": "D"})
        )
    return path


def test_read_admin_areas(osm_file: Path):
    areas = read_admin_areas(osm_file, 8, ["A", "B", "C", "D", "E"]).set_index("name")
    # C has a missing member, D has another admin level, E does not exist.
    assert list(areas.index) == ["A", "B"]
    assert areas.crs == "EPSG:4326"
    # Outer ring of A (0.01 x 0.01) minus its inner ring (0.002 x 0.002).
    assert areas.geometry["A"].area == pytest.approx(0.01**2 - 0.002**2)
    assert len(areas.geometry["A"].interiors) == 1
    assert areas.geometry["B"].area == pytest.approx(0.01**2)
    areas = read_admin_areas(osm_file, 6, ["D"])
    assert list(areas["name"]) == ["D"]


def test_step(tmp_path: Path, osm_file: Path):
    config = Config(
        {
            "main_directory": str(tmp_path / "output"),
            "osm_file": str(osm_file),
            "crs": "EPSG:2154",
            "simulation_area": {"osm_admin_level": 8, "osm_name": ["A", "B", "E"]},
        }
    )
    step = SimulationAreaFromOSMStep(config)
    assert step.is_defined()
    step.run()
    area = step.output["simulation_area"].read()
    assert len(area) == 1
    assert area.crs == "EPSG:2154"
    # Two disjoint polygons.
    assert area.geometry[0].geom_type == "MultiPolygon"


def test_step_single_name(tmp_path: Path, osm_file: Path):
    config = Config(
        {
            "main_directory": str(tmp_path / "output"),
            "osm_file": str(osm_file),
            "crs": "EPSG:2154",
            "simulation_area": {"osm_admin_level": 8, "osm_name": "B"},
        }
    )
    step = SimulationAreaFromOSMStep(config)
    step.run()
    assert step.output["simulation_area"].read().geometry[0].geom_type == "Polygon"


def test_step_no_area(tmp_path: Path, osm_file: Path):
    config = Config(
        {
            "main_directory": str(tmp_path / "output"),
            "osm_file": str(osm_file),
            "crs": "EPSG:2154",
            "simulation_area": {"osm_admin_level": 8, "osm_name": ["E"]},
        }
    )
    with pytest.raises(MetropyError):
        SimulationAreaFromOSMStep(config).run()
