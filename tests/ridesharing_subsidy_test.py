import polars as pl

from pymetropolis.metro_simulation.demand.trips import add_ridesharing_subsidy
from pymetropolis.modes import CarDriver, CarDriverWithPassengers, CarPassenger, CarRidesharing


def test_subsidy_added_for_ridesharing_modes():
    df = pl.DataFrame({"trip_id": [1, 2]})
    for mode in (CarDriverWithPassengers, CarPassenger, CarRidesharing):
        out = add_ridesharing_subsidy(df, mode, 2.0)
        assert out["constant_utility"].to_list() == [2.0, 2.0]


def test_subsidy_not_added_for_car_driver():
    df = pl.DataFrame({"trip_id": [1, 2]})
    out = add_ridesharing_subsidy(df, CarDriver, 2.0)
    assert "constant_utility" not in out.columns


def test_zero_subsidy_is_noop():
    df = pl.DataFrame({"trip_id": [1, 2]})
    out = add_ridesharing_subsidy(df, CarRidesharing, 0.0)
    assert "constant_utility" not in out.columns


def test_subsidy_added_on_top_of_existing_constant_utility():
    df = pl.DataFrame({"trip_id": [1, 2], "constant_utility": [-1.0, None]})
    out = add_ridesharing_subsidy(df, CarRidesharing, 2.0)
    assert out["constant_utility"].to_list() == [1.0, 2.0]
