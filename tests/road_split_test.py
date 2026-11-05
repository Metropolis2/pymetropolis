"""Tests for the identification of the primary part of the routes."""

import polars as pl
from polars.testing import assert_frame_equal

from pymetropolis.metro_demand.routing.road_split import find_first_last_primary

ROUTES = pl.DataFrame(
    {"trip_id": [1, 2, 3, 4, 5], "route": [[10, 20, 30, 40], [20, 99, 30], [40, 20], [99, 98], []]},
    schema={"trip_id": pl.Int64, "route": pl.List(pl.Int64)},
)


def test_find_first_last_primary():
    result = find_first_last_primary(ROUTES, {20, 30}).sort("trip_id")
    expected = pl.DataFrame(
        {"trip_id": [1, 2, 3], "first_idx_primary": [2, 1, 2], "last_idx_primary": [3, 3, 2]}
    )
    assert_frame_equal(result, expected)


def test_find_first_last_primary_no_primary_edge():
    result = find_first_last_primary(ROUTES, set())
    assert result.columns == ["trip_id", "first_idx_primary", "last_idx_primary"]
    assert result.is_empty()
