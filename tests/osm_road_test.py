"""Tests for the import of the road network from OpenStreetMap data.

The tests run on a small synthetic OSM file:

```
                    11
                    |          (9, private access, excluded)
                    4 ------- 9
                    |           (999, missing node)
    1 ----- 2 ----- 3 -- 5 -- 6 -- 7
    (way 100)       (102, oneway)  |  (103, closed roundabout 6-7-8-6)
                                   8 ----------- 10
                                        (107)
```

- Way 100 (1-2-3) is split at node 2 by way 101 (2-4-11).
- Way 101 is not split at node 4 as the ways crossing it there (104: private access, 106:
  references a missing node) are excluded.
- Way 102 (3-5-6) is oneway, with a stop sign (no direction) at node 5.
- Way 103 (6-7-8-6) is a closed roundabout, split at node 8 by way 107 (8-10).
- Node 3 has traffic signals (no direction), node 10 has a give-way sign (backward direction).
- Way 105 (1-4) is a footway (excluded).
"""

from pathlib import Path

import pyproj
import pytest
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_network.road_network.osm import M_TO_KM, OSMRoadNetworkImport

HIGHWAYS = ["primary", "secondary", "tertiary", "residential", "unclassified"]
ACCESS = ["yes", "permissive", "destination"]
CRS = pyproj.CRS("EPSG:2154")

NODES = {
    1: ((3.000, 45.000), {}),
    2: ((3.001, 45.000), {}),
    3: ((3.002, 45.000), {"highway": "traffic_signals"}),
    4: ((3.001, 45.001), {}),
    5: ((3.003, 45.000), {"highway": "stop"}),
    6: ((3.004, 45.000), {}),
    7: ((3.005, 45.0005), {}),
    8: ((3.005, 44.9995), {}),
    9: ((3.0015, 45.001), {}),
    10: ((3.010, 44.999), {"highway": "give_way", "direction": "backward"}),
    11: ((3.001, 45.002), {}),
}

WAYS = {
    100: ([1, 2, 3], {"highway": "primary", "maxspeed": "30 mph", "lanes": "4", "name": "A"}),
    101: (
        [2, 4, 11],
        {
            "highway": "secondary",
            "maxspeed": "FR:urban",
            "lanes:forward": "1",
            "lanes:backward": "2",
            "name": "",
            "ref": "D1",
        },
    ),
    102: ([3, 5, 6], {"highway": "tertiary", "oneway": "yes", "maxspeed": "50", "toll": "yes"}),
    103: ([6, 7, 8, 6], {"highway": "primary", "junction": "roundabout"}),
    104: ([4, 9], {"highway": "residential", "access": "private"}),
    105: ([1, 4], {"highway": "footway"}),
    106: ([4, 999], {"highway": "residential"}),
    107: ([8, 10], {"highway": "unclassified", "maxspeed:backward": "walk"}),
}

EXPECTED_IDS = {
    "100-0",
    "100-1",
    "100r-0",
    "100r-1",
    "101",
    "101r",
    "102",
    "103-0",
    "103-1",
    "107",
    "107r",
}


def write_osm(path: Path) -> Path:
    import osmium
    from osmium.osm.mutable import Node, Way

    with osmium.SimpleWriter(str(path)) as writer:
        for node_id, (location, tags) in NODES.items():
            writer.add_node(Node(id=node_id, location=location, tags=tags))
        for way_id, (nodes, tags) in WAYS.items():
            writer.add_way(Way(id=way_id, nodes=nodes, tags=tags))
    return path


@pytest.fixture
def osm_file(tmp_path: Path) -> Path:
    return write_osm(tmp_path / "test.osm.pbf")


def import_edges(
    osm_file: Path,
    highways: list[str] = HIGHWAYS,
    filter_polygon: BaseGeometry | None = None,
    reindex: bool = False,
):
    return OSMRoadNetworkImport(
        osm_file, highways, CRS, filter_polygon, ACCESS, reindex=reindex
    ).run()


def run(osm_file: Path, **kwargs):
    return import_edges(osm_file, **kwargs).set_index("edge_id")


def test_edges(osm_file: Path):
    edges = run(osm_file)
    assert set(edges.index) == EXPECTED_IDS
    assert edges.crs == CRS
    # Splitting and duplication.
    assert tuple(edges.loc["100-0", ["source", "target"]]) == (1, 2)
    assert tuple(edges.loc["100-1", ["source", "target"]]) == (2, 3)
    assert tuple(edges.loc["100r-0", ["source", "target"]]) == (2, 1)
    assert tuple(edges.loc["100r-1", ["source", "target"]]) == (3, 2)
    assert tuple(edges.loc["102", ["source", "target"]]) == (3, 6)
    # Closed roundabout is split at node 8.
    assert tuple(edges.loc["103-0", ["source", "target"]]) == (6, 8)
    assert tuple(edges.loc["103-1", ["source", "target"]]) == (8, 6)
    assert edges.loc[["103-0", "103-1"], "roundabout"].all()
    assert edges.loc[["103-0", "103-1"], "oneway"].all()
    assert not edges.loc["101", "oneway"]
    # Geometries.
    for edge_id, edge in edges.iterrows():
        assert edge.geometry.length == pytest.approx(edge["length"])
        assert edge["length"] > 0, edge_id
    assert edges.loc["100r-1"].geometry.equals(edges.loc["100-1"].geometry.reverse())
    assert len(edges.loc["103-0"].geometry.coords) == 3
    # Other attributes.
    assert edges.loc["100-0", "edge_type"] == "primary"
    assert edges.loc["100-0", "name"] == "A"
    assert edges.loc["101", "name"] == "D1"
    assert edges.loc["102", "toll"]
    assert not edges.loc["100-0", "toll"]
    assert edges["original_id"].to_dict()["100r-1"] == 100


def test_speed_limits_and_lanes(osm_file: Path):
    edges = run(osm_file)
    assert edges.loc["100-0", "speed_limit"] == pytest.approx(30 * M_TO_KM)
    assert edges.loc["100r-0", "speed_limit"] == pytest.approx(30 * M_TO_KM)
    assert edges.loc["100-0", "lanes"] == 2.0
    assert edges.loc["100r-0", "lanes"] == 2.0
    assert edges.loc["101", "speed_limit"] == 50.0
    assert edges.loc["101", "lanes"] == 1.0
    assert edges.loc["101r", "lanes"] == 2.0
    assert edges.loc["102", "speed_limit"] == 50.0
    assert edges["lanes"].isna()["102"]
    assert edges["speed_limit"].isna()["107"]
    assert edges.loc["107r", "speed_limit"] == 8.0


def test_node_features(osm_file: Path):
    edges = run(osm_file)
    signals = {e for e, v in edges["traffic_signals"].items() if v}
    # Node 3 is reached at the end of 100-1 and is inside the oneway way 102. It is not reached by
    # 100r-1 (which starts from it).
    assert signals == {"100-1", "102"}
    stops = {e for e, v in edges["stop"].items() if v}
    assert stops == {"102"}
    give_ways = {e for e, v in edges["give_way"].items() if v}
    assert give_ways == {"107r"}


def test_reindex(osm_file: Path):
    edges = import_edges(osm_file, reindex=True)
    assert sorted(edges["edge_id"]) == list(range(1, len(EXPECTED_IDS) + 1))
    # Edges are sorted by way id, direction and source.
    assert edges["original_id"].is_monotonic_increasing


def test_area_filter(osm_file: Path):
    transformer = pyproj.Transformer.from_crs("EPSG:4326", CRS, always_xy=True)
    area = Point(transformer.transform(3.010, 44.999)).buffer(50.0)
    edges = run(osm_file, filter_polygon=area)
    # Way 103 is no longer split as way 107 is the only way kept.
    assert set(edges.index) == {"107", "107r"}
    far_area = Point(transformer.transform(4.0, 44.0)).buffer(50.0)
    with pytest.raises(MetropyError):
        run(osm_file, filter_polygon=far_area)


def test_no_valid_way(osm_file: Path):
    with pytest.raises(MetropyError):
        import_edges(osm_file, highways=["motorway"])


def test_xml_file(tmp_path: Path, osm_file: Path):
    xml_file = write_osm(tmp_path / "test.osm")
    edges = run(xml_file)
    assert edges.equals(run(osm_file))
