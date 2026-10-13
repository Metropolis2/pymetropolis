import polars as pl
import pytest

from pymetropolis.metro_calibration.road.variables import (
    definition_to_expression,
    variable_definition_validator,
)
from pymetropolis.metro_common import MetropyError

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
