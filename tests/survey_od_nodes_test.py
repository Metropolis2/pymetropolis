import numpy as np
import polars as pl

from pymetropolis.metro_calibration.survey.travel_times import draw_distance_matched_od_points


def grid_candidates(zone_id: str, x0: float) -> pl.DataFrame:
    """Candidate points on a 20x20 grid (spacing 50m) covering a 1km x 1km zone starting at x0."""
    xs, ys = np.meshgrid(np.arange(20) * 50.0 + x0, np.arange(20) * 50.0)
    return pl.DataFrame({"location_id": [zone_id] * xs.size, "x": xs.ravel(), "y": ys.ravel()})


def candidates() -> pl.DataFrame:
    return pl.concat([grid_candidates("A", 0.0), grid_candidates("B", 2000.0)])


def trips() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trip_id": [1, 2, 3, 4, 5],
            "origin": ["A", "A", "B", "A", "A"],
            "destination": ["B", "B", "A", "A", "C"],
            "target": [1500.0, 2500.0, 2000.0, None, 1000.0],
        }
    )


def zone_of(x: pl.Series) -> list[str]:
    return ["A" if v < 1000.0 else "B" for v in x]


def test_distance_matching():
    df = draw_distance_matched_od_points(candidates(), trips(), 100, np.random.default_rng(0))
    # Trip 5 has no candidate point in its destination zone.
    assert df["trip_id"].to_list() == [1, 2, 3, 4]
    assert zone_of(df["origin_x"]) == ["A", "A", "B", "A"]
    assert zone_of(df["destination_x"]) == ["B", "B", "A", "A"]
    # The selected pairs match the target distances closely.
    errors = (df["od_distance"].head(3) - pl.Series([1500.0, 2500.0, 2000.0])).abs()
    assert (errors < 50.0).all()
    # The reported distance is consistent with the coordinates.
    dists = np.hypot(
        (df["origin_x"] - df["destination_x"]).to_numpy(),
        (df["origin_y"] - df["destination_y"]).to_numpy(),
    )
    assert np.allclose(dists, df["od_distance"].to_numpy())


def test_distance_matching_deterministic():
    cands = candidates()
    df1 = draw_distance_matched_od_points(cands, trips(), 10, np.random.default_rng(42))
    df2 = draw_distance_matched_od_points(cands, trips(), 10, np.random.default_rng(42))
    assert df1.equals(df2)


def test_distance_matching_special_locations():
    # Special locations are locations with a single candidate point.
    special = pl.DataFrame(
        {"location_id": ["special:1", "special:2"], "x": [100.0, 2600.0], "y": [100.0, 400.0]}
    )
    cands = pl.concat([candidates(), special])
    trips = pl.DataFrame(
        {
            "trip_id": [1, 2, 3],
            "origin": ["special:1", "A", "special:1"],
            "destination": ["B", "special:2", "special:2"],
            "target": [2200.0, 2000.0, 100.0],
        }
    )
    df = draw_distance_matched_od_points(cands, trips, 100, np.random.default_rng(0))
    assert df["trip_id"].to_list() == [1, 2, 3]
    # Origin only: the origin is the special location, the destination matches the distance.
    assert df.row(0, named=True)["origin_x"] == 100.0
    assert df.row(0, named=True)["origin_y"] == 100.0
    assert zone_of(df["destination_x"].head(1)) == ["B"]
    # Destination only.
    assert df.row(1, named=True)["destination_x"] == 2600.0
    assert df.row(1, named=True)["destination_y"] == 400.0
    assert zone_of(df["origin_x"].slice(1, 1)) == ["A"]
    assert ((df["od_distance"].head(2) - pl.Series([2200.0, 2000.0])).abs() < 50.0).all()
    # Both: the pair is fixed, whatever the target distance.
    row = df.row(2, named=True)
    assert (row["origin_x"], row["origin_y"], row["destination_x"], row["destination_y"]) == (
        100.0,
        100.0,
        2600.0,
        400.0,
    )
