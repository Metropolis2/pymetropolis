"""Tests for the import of the pedestrian and bicycle networks from OpenStreetMap data.

The tests run on a small synthetic OSM file:

```
    1 ----- 2 ----- 3 ----- 5 ----- 6
    (way 200)  |    (202, oneway) (203, oneway, contraflow)
               |
               4 ----- 8
             (201)   (205, crossing)
```

- Way 200 (1-2-3) is a residential road, split at node 2 by the cycleway 201 (2-4).
- Way 202 (3-5) is a oneway residential road.
- Way 203 (5-6) is a oneway residential road, with contraflow for bicycles.
- Way 204 (1-7) is a footway, with no bicycle access, and way 206 (3-7) is a private service road.
- Way 205 (4-8) is a crossing, way 207 (6-8) is a pedestrian area.
- Node 2 has a bump, node 3 has traffic signals (no direction).
"""

from pathlib import Path

import pyproj
import pytest

from pymetropolis.metro_network.bicycle_network.osm import OSMBicycleNetworkImport
from pymetropolis.metro_network.pedestrian_network.osm import OSMPedestrianNetworkImport

HIGHWAYS = ["residential", "service", "cycleway", "footway", "crossing", "pedestrian"]
CRS = pyproj.CRS("EPSG:2154")

NODES = {
    1: ((3.000, 45.000), {}),
    2: ((3.001, 45.000), {"traffic_calming": "bump"}),
    3: ((3.002, 45.000), {"highway": "traffic_signals"}),
    4: ((3.001, 45.001), {}),
    5: ((3.003, 45.000), {}),
    6: ((3.004, 45.000), {}),
    7: ((3.000, 45.001), {}),
    8: ((3.002, 45.001), {}),
}

WAYS = {
    200: (
        [1, 2, 3],
        {
            "highway": "residential",
            "surface": "asphalt",
            "smoothness": "excellent",
            "maxspeed": "30",
        },
    ),
    201: ([2, 4], {"highway": "cycleway", "surface": "gravel"}),
    202: ([3, 5], {"highway": "residential", "oneway": "yes", "tracktype": "grade2"}),
    203: ([5, 6], {"highway": "residential", "oneway": "yes", "oneway:bicycle": "no"}),
    204: ([1, 7], {"highway": "footway", "bicycle": "no"}),
    205: ([4, 8], {"highway": "crossing"}),
    206: ([3, 7], {"highway": "service", "access": "private"}),
    207: ([6, 8, 6], {"highway": "pedestrian", "area": "yes"}),
}


@pytest.fixture
def osm_file(tmp_path: Path) -> Path:
    import osmium
    from osmium.osm.mutable import Node, Way

    path = tmp_path / "test.osm.pbf"
    with osmium.SimpleWriter(str(path)) as writer:
        for node_id, (location, tags) in NODES.items():
            writer.add_node(Node(id=node_id, location=location, tags=tags))
        for way_id, (nodes, tags) in WAYS.items():
            writer.add_way(Way(id=way_id, nodes=nodes, tags=tags))
    return path


def test_pedestrian(osm_file: Path):
    edges = OSMPedestrianNetworkImport(osm_file, HIGHWAYS, CRS, None).run()
    edges = edges.set_index("edge_id")
    # All ways are duplicated, except the private one (206) and the area (207).
    assert set(edges["original_id"]) == {200, 201, 202, 203, 204, 205}
    assert set(edges.index) == {
        "200-0",
        "200-1",
        "200r-0",
        "200r-1",
        *(f"{way}{r}" for way in (201, 202, 203, 204, 205) for r in ("", "r")),
    }
    assert list(edges.columns) == [
        "source",
        "target",
        "edge_type",
        "name",
        "original_id",
        "geometry",
        "length",
    ]


def test_bicycle(osm_file: Path):
    edges = OSMBicycleNetworkImport(osm_file, HIGHWAYS, CRS, None).run()
    edges = edges.set_index("edge_id")
    # No bicycle access on way 204, the private service road (206) and the pedestrian area (207)
    # are still valid for bicycles.
    assert 204 not in set(edges["original_id"])
    assert {"206", "206r", "207-0", "207-1", "207r-0", "207r-1"} <= set(edges.index)
    # Oneway road: no backward edge. Oneway road with contraflow: backward edge.
    assert "202" in edges.index and "202r" not in edges.index
    assert {"203", "203r"} <= set(edges.index)
    assert edges.loc["200-0", "type"] == "road"
    assert edges.loc["201", "type"] == "track"
    assert edges.loc["205", "type"] == edges.loc["205r", "type"] == "crossing"
    # Features.
    assert edges.loc["200-0", "speed_limit"] == 30.0
    assert edges.loc["200-0", "quality"] == 10
    assert edges.loc["201", "quality"] == 4
    assert edges.loc["202", "quality"] == 5
    assert edges.loc["203", "quality"] == 8
    bumps = {e for e, v in edges["has_bump"].items() if v}
    assert bumps == {"200-0", "200-1", "200r-0", "200r-1", "201", "201r"}
    signals = {e for e, v in edges["traffic_signals"].items() if v}
    assert signals == {"200-1", "202", "206r"}
