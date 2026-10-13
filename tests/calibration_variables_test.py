import copy
from pathlib import Path

import geopandas as gpd
import numpy as np
import polars as pl
import pytest
from shapely.geometry import LineString

from pymetropolis.metro_calibration.road.files import RoadEdgesVariablesFile
from pymetropolis.metro_calibration.road.variables import (
    DEFAULT_DEFINITIONS,
    RoadEdgesVariablesStep,
    definition_to_expression,
    variable_definition_validator,
)
from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_network.road_network.files import RoadEdgesCleanFile
from pymetropolis.metro_pipeline.config import Config

# Expected labels are the ones produced by polars' `cut` (with whitespaces removed), which was used
# before polars 2.0: they are kept identical as they end up in the names of the dummy variables.
VALUES = [-3.0, 0.0, 0.1, 2.0, 2.5, 6.5, 7.0, 1e21, None]


@pytest.mark.parametrize(
    ("cuts", "left_closed", "expected"),
    [
        (
            [6.5, 2],
            False,
            [
                "(-inf,2]",
                "(-inf,2]",
                "(-inf,2]",
                "(-inf,2]",
                "(2,6.5]",
                "(2,6.5]",
                "(6.5,inf]",
                "(6.5,inf]",
                None,
            ],
        ),
        (
            [6.5, 2],
            True,
            [
                "[-inf,2)",
                "[-inf,2)",
                "[-inf,2)",
                "[2,6.5)",
                "[2,6.5)",
                "[6.5,inf)",
                "[6.5,inf)",
                "[6.5,inf)",
                None,
            ],
        ),
        (
            [1e-7, 1e20],
            False,
            [
                "(-inf,0.0000001]",
                "(-inf,0.0000001]",
                "(0.0000001,100000000000000000000]",
                "(0.0000001,100000000000000000000]",
                "(0.0000001,100000000000000000000]",
                "(0.0000001,100000000000000000000]",
                "(0.0000001,100000000000000000000]",
                "(100000000000000000000,inf]",
                None,
            ],
        ),
    ],
)
def test_intervals_variable(cuts: list[float], left_closed: bool, expected: list[str | None]):
    df = pl.DataFrame({"x": VALUES}, schema={"x": pl.Float64})
    definition = {"type": "intervals", "cuts": cuts, "left_closed": left_closed}
    res = df.select(definition_to_expression("x", definition))["x"]
    assert res.dtype == pl.Categorical
    assert res.cast(pl.String).to_list() == expected


def test_intervals_variable_integer_column():
    df = pl.DataFrame({"x": [10, 30, 31, 70, 100]}, schema={"x": pl.Int32})
    definition = {"type": "intervals", "cuts": [30, 50.5, 70]}
    res = df.select(definition_to_expression("x", definition))["x"]
    assert res.cast(pl.String).to_list() == [
        "(-inf,30]",
        "(-inf,30]",
        "(30,50.5]",
        "(50.5,70]",
        "(70,inf]",
    ]


@pytest.mark.parametrize("cuts", [[1, "a"], ["a"], "1,2", 3])
def test_intervals_validator_rejects_invalid_cuts(cuts):
    with pytest.raises(MetropyError, match="list of numbers expected"):
        variable_definition_validator({"x": {"type": "intervals", "cuts": cuts}})


def test_intervals_validator_accepts_numbers():
    definition = {"x": {"type": "intervals", "cuts": [1, 2.5]}}
    assert variable_definition_validator(definition) == definition


def write_clean_edges(main_directory: Path):
    """Writes a clean-edges file with two edges, without `edge_type` (and no urban-flag file)."""
    n = 2
    edges = gpd.GeoDataFrame(
        {
            "edge_id": [1, 2],
            "source": [1, 2],
            "target": [2, 1],
            "length": [1000.0, 500.0],
            "speed_limit": [50.0, 30.0],
            "default_speed_limit": [False] * n,
            "lanes": [1.0, 2.0],
            "hov_lanes": [0.0] * n,
            "default_lanes": [False] * n,
            "oneway": [True] * n,
            "toll": [False] * n,
            "roundabout": [False] * n,
            "give_way": [False] * n,
            "stop": [False] * n,
            "traffic_signals": [True, False],
            "source_in_degree": np.ones(n, dtype=np.uint64),
            "source_out_degree": np.ones(n, dtype=np.uint64),
            "target_in_degree": np.ones(n, dtype=np.uint64),
            "target_out_degree": np.ones(n, dtype=np.uint64),
        },
        geometry=[LineString([(0, 0), (1000, 0)]), LineString([(1000, 0), (0, 0)])],
        crs="EPSG:2154",
    )
    RoadEdgesCleanFile.from_dir(main_directory).write(edges)


def test_edges_variables_without_custom_definitions(tmp_path: Path):
    # The `urban` default variable is not available: it must be skipped, not crash.
    write_clean_edges(tmp_path)
    RoadEdgesVariablesStep(Config({"main_directory": str(tmp_path)})).run()
    df = RoadEdgesVariablesFile.from_dir(tmp_path).read()
    assert "urban" not in df.columns
    assert df["base_free_flow_tt"].to_list() == pytest.approx([72.0, 60.0])
    assert df["traffic_signals"].to_list() == [1.0, 0.0]


def test_edges_variables_do_not_mutate_defaults(tmp_path: Path):
    defaults = copy.deepcopy(DEFAULT_DEFINITIONS)
    write_clean_edges(tmp_path)
    config = Config(
        {
            "main_directory": str(tmp_path),
            "road_network": {"calibration_variables": {"lanes": {"type": "boolean"}}},
        }
    )
    RoadEdgesVariablesStep(config).run()
    assert DEFAULT_DEFINITIONS == defaults
