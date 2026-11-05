"""Tests for the reading of Eqasim trips and homes, from both GeoParquet and GeoPackage files."""

from datetime import timedelta
from pathlib import Path
from typing import TypedDict

import geopandas as gpd
import pytest
from shapely.geometry import LineString, Point, box

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_demand.population.eqasim import read_homes, read_trips

CRS = "EPSG:2154"
HOME_1 = (650_000.0, 6_860_000.0)
HOME_2 = (652_000.0, 6_860_000.0)
FAR_HOME = (900_000.0, 6_500_000.0)
WORK = (651_000.0, 6_861_000.0)
SHOP = (652_000.0, 6_861_000.0)
# Contains HOME_1 and HOME_2 but not FAR_HOME.
AREA = box(649_000, 6_859_000, 653_000, 6_861_000)


class Source(TypedDict, total=False):
    """Keyword arguments selecting the input file of `read_trips` / `read_homes`."""

    geoparquet_file: Path
    gpkg_file: Path


def write(gdf: gpd.GeoDataFrame, directory: Path, name: str, file_format: str) -> Source:
    if file_format == "geoparquet":
        path = directory / f"{name}.parquet"
        gdf.to_parquet(path)
        return {"geoparquet_file": path}
    path = directory / f"{name}.gpkg"
    gdf.to_file(path, driver="GPKG")
    return {"gpkg_file": path}


@pytest.fixture(params=["geoparquet", "gpkg"])
def trips_source(request: pytest.FixtureRequest, tmp_path: Path) -> Source:
    # The rows are not sorted: `read_trips` is responsible for it.
    trips = gpd.GeoDataFrame(
        {
            "person_id": [2, 1, 1, 2, 1],
            "trip_index": [1, 1, 0, 0, 2],
            "departure_time": [50_000.0, 60_000.0, 28_800.0, 36_000.0, 70_000.0],
            "arrival_time": [51_000.0, 61_000.0, 30_000.0, 37_000.0, 71_000.0],
            "preceding_purpose": ["shop", "work", "home", "home", "home"],
            "following_purpose": ["home", "home", "work", "shop", "shop"],
        },
        geometry=[
            LineString([SHOP, HOME_2]),
            LineString([WORK, HOME_1]),
            LineString([HOME_1, WORK]),
            LineString([HOME_2, SHOP]),
            LineString([HOME_1, SHOP]),
        ],
        crs=CRS,
    )
    return write(trips, tmp_path, "trips", request.param)


@pytest.fixture(params=["geoparquet", "gpkg"])
def homes_source(request: pytest.FixtureRequest, tmp_path: Path) -> Source:
    homes = gpd.GeoDataFrame(
        {"household_id": [1, 2, 3]},
        geometry=[Point(HOME_1), Point(HOME_2), Point(FAR_HOME)],
        crs=CRS,
    )
    return write(homes, tmp_path, "homes", request.param)


def coordinates(gdf: gpd.GeoDataFrame) -> dict[str, tuple[float, float]]:
    return {
        trip_id: (point.x, point.y)
        for trip_id, point in zip(gdf["trip_id"], gdf.geometry, strict=True)
    }


def test_read_trips(trips_source: Source):
    trips, _, _ = read_trips(**trips_source)
    assert trips["trip_id"].to_list() == ["1-0", "1-1", "1-2", "2-0", "2-1"]
    assert trips["person_id"].to_list() == [1, 1, 1, 2, 2]
    assert trips["trip_index"].to_list() == [1, 2, 3, 1, 2]
    # A new tour starts each time the person leaves home.
    assert trips["tour_id"].to_list() == ["1-1", "1-1", "1-2", "2-1", "2-1"]
    assert trips["origin_purpose_group"].to_list() == ["home", "work", "home", "home", "shopping"]
    assert trips["destination_purpose_group"].to_list() == [
        "work",
        "home",
        "shopping",
        "shopping",
        "home",
    ]
    assert trips["departure_time"][0] == timedelta(seconds=28_800)
    assert trips["arrival_time"][0] == timedelta(seconds=30_000)
    assert trips["origin_activity_duration"].to_list() == [
        None,
        timedelta(seconds=30_000),
        timedelta(seconds=9_000),
        None,
        timedelta(seconds=13_000),
    ]
    assert trips["destination_activity_duration"].to_list() == [
        timedelta(seconds=30_000),
        timedelta(seconds=9_000),
        None,
        timedelta(seconds=13_000),
        None,
    ]


def test_read_trips_origins_destinations(trips_source: Source):
    _, origins, destinations = read_trips(**trips_source)
    assert origins.crs == CRS
    assert destinations.crs == CRS
    assert coordinates(origins) == {
        "1-0": HOME_1,
        "1-1": WORK,
        "1-2": HOME_1,
        "2-0": HOME_2,
        "2-1": SHOP,
    }
    assert coordinates(destinations) == {
        "1-0": WORK,
        "1-1": HOME_1,
        "1-2": SHOP,
        "2-0": SHOP,
        "2-1": HOME_2,
    }


def test_read_trips_person_ids(trips_source: Source):
    trips, origins, destinations = read_trips(**trips_source, person_ids={2})
    assert trips["trip_id"].to_list() == ["2-0", "2-1"]
    assert set(origins["trip_id"]) == {"2-0", "2-1"}
    assert set(destinations["trip_id"]) == {"2-0", "2-1"}


def test_read_homes(homes_source: Source):
    homes = read_homes(**homes_source).sort_values("household_id")
    assert homes.crs == CRS
    assert homes["household_id"].to_list() == [1, 2, 3]
    assert [(p.x, p.y) for p in homes.geometry] == [HOME_1, HOME_2, FAR_HOME]


def test_read_homes_filter_polygon(homes_source: Source):
    homes = read_homes(**homes_source, filter_polygon=AREA)
    assert sorted(homes["household_id"]) == [1, 2]


def test_read_homes_no_home_in_area(homes_source: Source):
    with pytest.raises(MetropyError, match="No household within the simulation area"):
        read_homes(**homes_source, filter_polygon=box(0, 0, 1, 1))


def test_read_homes_fraction(homes_source: Source):
    first = read_homes(**homes_source, fraction=0.5, random_seed=1)
    second = read_homes(**homes_source, fraction=0.5, random_seed=1)
    assert len(first) == 1
    assert first["household_id"].to_list() == second["household_id"].to_list()


def test_read_homes_fraction_too_small(homes_source: Source):
    with pytest.raises(MetropyError, match="No household selected"):
        read_homes(**homes_source, fraction=0.0)
