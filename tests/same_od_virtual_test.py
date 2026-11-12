import polars as pl

from pymetropolis.metro_simulation.demand.trips import (
    clean_trips,
    generate_bicycle_trips,
    generate_public_transit_trips,
    generate_walking_trips,
)


class StubFile:
    def __init__(self, df: pl.DataFrame):
        self.df = df

    def read(self) -> pl.DataFrame:
        return self.df

    def exists(self) -> bool:
        return True


def make_trips() -> pl.DataFrame:
    # Tour 1: trip "a" has the same origin and destination, trip "b" does not.
    # Tour 2: a single normal trip.
    return pl.DataFrame(
        {
            "trip_id": ["a", "b", "c"],
            "tour_id": [1, 1, 2],
            "destination_activity_duration": pl.Series([None, None, None], dtype=pl.Duration),
        }
    )


def make_distances() -> pl.DataFrame:
    return pl.DataFrame({"trip_id": ["a", "b", "c"], "od_distance": [0.0, 100.0, 50.0]})


def test_clean_trips_flags_same_od():
    df = clean_trips(make_trips(), make_distances())
    assert dict(zip(df["trip_id"], df["same_od"])) == {"a": True, "b": False, "c": False}


def test_clean_trips_without_distances_flags_nothing():
    df = clean_trips(make_trips())
    assert df["same_od"].to_list() == [False, False, False]


def test_clean_trips_null_distance_is_not_same_od():
    distances = pl.DataFrame({"trip_id": ["a", "b"], "od_distance": [None, 0.0]}).cast(
        {"od_distance": pl.Float64}
    )
    df = clean_trips(make_trips(), distances)
    assert dict(zip(df["trip_id"], df["same_od"])) == {"a": False, "b": True, "c": False}


def test_public_transit_keeps_tour_with_same_od_trip():
    df = clean_trips(make_trips(), make_distances())
    # No itinerary for trip "a".
    itineraries = pl.DataFrame(
        {"trip_id": ["a", "b", "c"], "travel_time": [None, 600.0, 300.0]}
    ).with_columns(travel_time=pl.duration(seconds=pl.col("travel_time")))
    out = generate_public_transit_trips(df, StubFile(itineraries))
    times = dict(zip(out["trip_id"], out["class.travel_time"]))
    assert times == {"a": 0.0, "b": 600.0, "c": 300.0}
    assert set(out["class.type"]) == {"Virtual"}


def test_public_transit_still_drops_tour_with_missing_itinerary():
    df = clean_trips(make_trips(), make_distances())
    itineraries = pl.DataFrame(
        {"trip_id": ["a", "b", "c"], "travel_time": [0.0, None, 300.0]}
    ).with_columns(travel_time=pl.duration(seconds=pl.col("travel_time")))
    out = generate_public_transit_trips(df, StubFile(itineraries))
    assert out["trip_id"].to_list() == ["c"]


def test_walking_and_bicycle_ignore_snap_time_for_same_od():
    df = clean_trips(make_trips(), make_distances())
    for fn, col in ((generate_walking_trips, "walking"), (generate_bicycle_trips, "bicycle")):
        tts = pl.DataFrame(
            {"trip_id": ["a", "b", "c"], f"{col}_travel_time": [3600.0, 120.0, 60.0]}
        ).with_columns(
            pl.duration(seconds=pl.col(f"{col}_travel_time")).alias(f"{col}_travel_time")
        )
        out = fn(df, StubFile(tts))
        times = dict(zip(out["trip_id"], out["class.travel_time"]))
        assert times == {"a": 0.0, "b": 120.0, "c": 60.0}
