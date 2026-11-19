import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import polars as pl
import pytest
from shapely.geometry import LineString

from pymetropolis.metro_calibration.road.capacities.factors import (
    CapacitiesFromFactorsStep,
    CustomCapacityFactorsStep,
    validate_capacity_factors,
)
from pymetropolis.metro_calibration.road.capacities.files import (
    CapacityFactorsFile,
    EdgeCapacityFactorsFile,
)
from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_network.road_network.files import (
    RoadEdgesCapacitiesFile,
    RoadEdgesCleanFile,
)
from pymetropolis.metro_pipeline.config import Config

FACTORS = {
    "edge_type=motorway": 0.6,
    "edge_type=local": 0.25,
    "lanes=(1,inf]": 0.9,
    "traffic_signals=true": 0.7,
    "roundabout=true": 0.9,
}


def test_validate_capacity_factors():
    assert validate_capacity_factors({"lanes=(1,inf]": 1, "edge_type=motorway": 0.5}) == {
        "lanes=(1,inf]": 1.0,
        "edge_type=motorway": 0.5,
    }


@pytest.mark.parametrize(
    "value",
    [
        [0.5],
        {},
        {"motorway": 0.5},
        {"=motorway": 0.5},
        {"edge_type=": 0.5},
        {"edge_type=motorway": "0.5"},
        {"edge_type=motorway": 0.0},
        {"edge_type=motorway": -1.0},
        {"edge_type=motorway": True},
        {"edge_type=motorway": float("inf")},
    ],
)
def test_validate_capacity_factors_rejects_invalid(value):
    with pytest.raises(MetropyError):
        validate_capacity_factors(value)


def write_clean_edges(main_directory: Path, n: int):
    edges = gpd.GeoDataFrame(
        {
            "edge_id": list(range(1, n + 1)),
            "source": list(range(1, n + 1)),
            "target": list(range(2, n + 2)),
            "length": [1000.0] * n,
            "speed_limit": [50.0] * n,
            "default_speed_limit": [False] * n,
            "lanes": [1.0] * n,
            "hov_lanes": [0.0] * n,
            "default_lanes": [False] * n,
            "oneway": [True] * n,
            "toll": [False] * n,
            "roundabout": [False] * n,
            "give_way": [False] * n,
            "stop": [False] * n,
            "traffic_signals": [False] * n,
            "source_in_degree": np.ones(n, dtype=np.uint64),
            "source_out_degree": np.ones(n, dtype=np.uint64),
            "target_in_degree": np.ones(n, dtype=np.uint64),
            "target_out_degree": np.ones(n, dtype=np.uint64),
        },
        geometry=[LineString([(i, 0), (i + 1, 0)]) for i in range(n)],
        crs="EPSG:2154",
    )
    RoadEdgesCleanFile.from_dir(main_directory).write(edges)


def run_steps(tmp_path: Path, calibration: dict) -> dict:
    write_clean_edges(tmp_path, 5)
    edge_factors = pl.DataFrame(
        {
            "edge_id": [1, 1, 1, 2, 3, 3],
            "factor": [
                "edge_type=motorway",
                "lanes=(1,inf]",
                "traffic_signals=true",
                "edge_type=local",
                # Factors not defined in the JSON file.
                "edge_type=primary",
                "target_in_degree=(1,2]",
            ],
        }
    )
    # Edge 4 has no factor at all, edge 5 is not in the edge-factors file.
    edge_factors = edge_factors.filter(pl.col("edge_id") != 5)
    EdgeCapacityFactorsFile.from_dir(tmp_path).write(edge_factors)
    factors_path = tmp_path / "factors.json"
    factors_path.write_text(json.dumps(FACTORS))
    config = Config(
        {
            "main_directory": str(tmp_path),
            "calibration": {"capacity": {"custom_factors_file": str(factors_path), **calibration}},
        }
    )
    CustomCapacityFactorsStep(config).run()
    assert CapacityFactorsFile.from_dir(tmp_path).read() == FACTORS
    CapacitiesFromFactorsStep(config).run()
    df = RoadEdgesCapacitiesFile.from_dir(tmp_path).read().sort("edge_id")
    return dict(zip(df["edge_id"].to_list(), df["capacity"].to_list(), strict=True))


def test_capacities_from_factors(tmp_path: Path):
    capacities = run_steps(tmp_path, {})
    assert capacities[1] == pytest.approx(3000 * 0.6 * 0.9 * 0.7)
    assert capacities[2] == pytest.approx(3000 * 0.25)
    assert capacities[3] is None
    assert capacities[4] is None
    assert capacities[5] is None


def test_capacities_from_factors_base_capacity(tmp_path: Path):
    capacities = run_steps(tmp_path, {"base_capacity": 2000})
    assert capacities[2] == pytest.approx(2000 * 0.25)


def test_custom_factors_invalid_json(tmp_path: Path):
    factors_path = tmp_path / "factors.json"
    factors_path.write_text("{not json")
    config = Config(
        {
            "main_directory": str(tmp_path),
            "calibration": {"capacity": {"custom_factors_file": str(factors_path)}},
        }
    )
    with pytest.raises(MetropyError):
        CustomCapacityFactorsStep(config).run()
