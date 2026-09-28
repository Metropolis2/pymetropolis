import zipfile
from datetime import date
from datetime import timedelta as td

import polars as pl
import pytest

from pymetropolis.metro_demand.modes.park_and_ride.transfer_stops import (
    park_and_ride_car_legs,
    park_and_ride_car_trips,
    park_and_ride_eligible_trips,
)
from pymetropolis.metro_network.public_transit.gtfs import read_gtfs_stops_and_routes
from pymetropolis.metro_results.demand.postprocess import merge_park_and_ride_legs
from pymetropolis.metro_simulation.demand.trips import clean_trips, generate_park_and_ride_trips


class FakeFile:
    """Minimal stand-in for a MetroFile, holding a DataFrame in memory."""

    def __init__(self, df: pl.DataFrame | None):
        self.df = df

    def read(self):
        return self.df

    def read_as_df(self):
        return self.df

    def exists(self):
        return self.df is not None


def seconds(values):
    return [td(seconds=v) if v is not None else None for v in values]


# Tour "1-1": home -> work -> shop -> home (3 trips); tour "2-1": single trip home -> work;
# tour "3-1": work -> home (does not start from home).
TRIPS = pl.DataFrame(
    {
        "trip_id": ["1-1", "1-2", "1-3", "2-1", "3-1"],
        "person_id": [1, 1, 1, 2, 3],
        "household_id": [1, 1, 1, 2, 3],
        "trip_index": [1, 2, 3, 1, 1],
        "tour_id": ["1-1", "1-1", "1-1", "2-1", "3-1"],
        "origin_purpose_group": ["home", "work", "shop", "home", "work"],
        "destination_purpose_group": ["work", "shop", "home", "work", "home"],
        "destination_activity_duration": seconds([8 * 3600, 3600, None, 8 * 3600, None]),
        "has_car": [True] * 5,
        "has_driving_license": [True] * 5,
    }
)


def test_eligible_tours():
    tours = park_and_ride_eligible_trips(TRIPS)
    assert tours["tour_id"].to_list() == ["1-1", "2-1"]
    assert tours["first_trip_id"].to_list() == ["1-1", "2-1"]


def test_car_trips_and_legs():
    car = park_and_ride_car_trips(TRIPS, pl.Series(["1-1", "2-1"])).sort("trip_id")
    # First and last trip of tour "1-1"; single-trip tour "2-1" has only an outbound car part.
    assert car["trip_id"].to_list() == ["1-1", "1-3", "2-1"]
    assert car["is_outbound"].to_list() == [True, False, True]
    legs = park_and_ride_car_legs(TRIPS, pl.Series(["1-1", "2-1"])).sort("trip_id")
    assert legs["car_leg_id"].to_list() == ["1-1-1", "1-3-2", "2-1-1"]


def generate(transfer_time=300.0):
    df = clean_trips(TRIPS)
    return generate_park_and_ride_trips(
        df,
        TRIPS,
        pr_stops_file=FakeFile(pl.DataFrame({"tour_id": ["1-1", "2-1"]})),
        primary_trips_file=FakeFile(
            pl.DataFrame(
                {
                    "trip_id": ["1-1", "1-3"],
                    "access_node": [10, 20],
                    "egress_node": [11, 21],
                    "access_time": seconds([60, 70]),
                    "egress_time": seconds([30, 40]),
                }
            )
        ),
        secondary_trips_file=FakeFile(
            pl.DataFrame({"trip_id": ["2-1"], "free_flow_travel_time": seconds([120])})
        ),
        park_and_ride_itineraries_file=FakeFile(
            pl.DataFrame(
                {
                    "trip_id": ["1-1", "1-3", "2-1"],
                    "travel_time": seconds([900, 1000, None]),
                    "generalized_time": seconds([1200, 1300, None]),
                }
            )
        ),
        pt_itineraries_file=FakeFile(
            pl.DataFrame(
                {
                    "trip_id": ["1-2"],
                    "travel_time": seconds([600]),
                    "generalized_time": seconds([700]),
                }
            )
        ),
        pref_file=FakeFile(
            pl.DataFrame(
                {
                    "tour_id": ["1-1", "2-1"],
                    "park_and_ride_cst": [1.0, 1.0],
                    "public_transit_vot": [10.0, 10.0],
                    "car_vot": [15.0, 15.0],
                }
            )
        ),
        tstars_file=FakeFile(
            pl.DataFrame(
                {"trip_id": ["1-1", "1-2", "1-3"], "tstar": seconds([30000, 60000, 70000])}
            )
        ),
        schedule_pref_file=None,
        fuel_file=FakeFile(pl.DataFrame({"trip_id": ["1-1", "1-3"], "fuel_cost": [1.5, 1.6]})),
        transfer_time=transfer_time,
    )


def test_generate_trips_legs():
    out = generate()
    # Tour "2-1" is discarded (no public-transit itinerary); tour "3-1" is not eligible.
    assert out["agent_id"].unique().to_list() == ["1-1"]
    assert out["trip_id"].to_list() == ["1-1-1", "1-1-2", "1-2", "1-3-1", "1-3-2"]
    assert out["class.type"].to_list() == ["Road", "Virtual", "Virtual", "Virtual", "Road"]
    assert out["class.vehicle"].to_list() == [
        "car_driver_alone",
        None,
        None,
        None,
        "car_driver_alone",
    ]
    assert out["class.travel_time"].to_list() == [None, 900.0, 600.0, 1000.0, None]


def test_generate_trips_stopping_times():
    out = generate()
    # Outbound car leg: egress + transfer. Outbound PT leg: activity duration (8h).
    # Intermediary PT trip: activity duration (1h). Inbound PT leg: transfer + car access.
    # Inbound car leg: egress (last trip of the tour).
    assert out["stopping_time"].to_list() == [330.0, 28800.0, 3600.0, 370.0, 40.0]


def test_generate_trips_utilities():
    out = generate()
    utilities = out["constant_utility"].to_list()
    # Car legs: -car_vot * (access + egress) - fuel cost.
    assert utilities[0] == pytest.approx(-15 / 3600 * 90 - 1.5)
    assert utilities[4] == pytest.approx(-15 / 3600 * 110 - 1.6)
    # PT legs: -pt_vot * (generalized time + transfer time), no transfer for intermediary trips.
    assert utilities[1] == pytest.approx(-10 / 3600 * (1200 + 300))
    assert utilities[2] == pytest.approx(-10 / 3600 * 700)
    assert utilities[3] == pytest.approx(-10 / 3600 * (1300 + 300))
    # Car legs are valued with the car value of time.
    assert out["alpha"].to_list()[0] == pytest.approx(15 / 3600)


def test_generate_trips_schedule():
    out = generate()
    # tstar is set on the last leg of each trip only (minus egress time for inbound car legs).
    assert out["schedule_utility.tstar"].to_list() == [None, 30000.0, 60000.0, None, 69960.0]


def test_generate_trips_excludes_joint_tours():
    df = clean_trips(TRIPS.with_columns(joint_tour=pl.col("tour_id") == "1-1"))
    out = generate_park_and_ride_trips(
        df,
        TRIPS,
        pr_stops_file=FakeFile(pl.DataFrame({"tour_id": ["1-1"]})),
        primary_trips_file=FakeFile(
            pl.DataFrame(
                {
                    "trip_id": ["1-1"],
                    "access_node": [1],
                    "egress_node": [2],
                    "access_time": seconds([0]),
                    "egress_time": seconds([0]),
                }
            )
        ),
        secondary_trips_file=FakeFile(
            pl.DataFrame(
                {"trip_id": [], "free_flow_travel_time": []},
                schema_overrides={"trip_id": pl.String, "free_flow_travel_time": pl.Duration("us")},
            )
        ),
        park_and_ride_itineraries_file=FakeFile(
            pl.DataFrame({"trip_id": ["1-1", "1-3"], "travel_time": seconds([60, 60])})
        ),
        pt_itineraries_file=FakeFile(
            pl.DataFrame({"trip_id": ["1-2"], "travel_time": seconds([60])})
        ),
    )
    assert out.is_empty()


def test_merge_legs():
    df = pl.DataFrame(
        {
            "trip_id": ["1-1-1", "1-1-2", "1-2", "1-3-1", "1-3-2", "4-1"],
            "tour_id": ["1-1"] * 5 + ["4-1"],
            "mode": ["park_and_ride"] * 5 + ["car_driver"],
            "is_road": [True, False, False, False, True, True],
            "departure_time": seconds([25200, 25920, 61200, 64800, 66600, 28800]),
            "arrival_time": seconds([25560, 28800, 63000, 66240, 68400, 32400]),
            "utility": [-1.0, -2.0, -3.0, -4.0, -5.0, -6.0],
            "route_length": [5000.0, None, None, None, 6000.0, 10000.0],
            "vehicle_id": ["car_driver_alone", None, None, None, "car_driver_alone", "car"],
        }
    )
    trips = pl.DataFrame({"trip_id": ["1-1", "1-2", "1-3", "4-1"]})
    out = merge_park_and_ride_legs(df, trips).sort("trip_id")
    assert out["trip_id"].to_list() == ["1-1", "1-2", "1-3", "4-1"]
    assert out["tour_id"].to_list() == ["1-1", "1-1", "1-1", "4-1"]
    assert out["departure_time"].to_list() == seconds([25200, 61200, 64800, 28800])
    assert out["arrival_time"].to_list() == seconds([28800, 63000, 68400, 32400])
    assert out["utility"].to_list() == [-3.0, -3.0, -9.0, -6.0]
    assert out["route_length"].to_list() == [5000.0, None, 6000.0, 10000.0]
    assert out["is_road"].to_list() == [True, False, True, True]
    assert out["vehicle_id"].to_list() == ["car_driver_alone", None, "car_driver_alone", "car"]


def write_gtfs(path, tables: dict[str, str]):
    with zipfile.ZipFile(path, "w") as z:
        for name, content in tables.items():
            z.writestr(f"{name}.txt", content)


GTFS_TABLES = {
    "agency": "agency_id,agency_name\nA,Agency\n",
    "routes": (
        "route_id,route_short_name,route_long_name,route_type,route_color\n"
        "R1,TER,,2,FF0000\n"
        "R2,,Bus 2,3,\n"
    ),
    "trips": "route_id,service_id,trip_id\nR1,WEEK,T1\nR2,SUN,T2\n",
    "stop_times": "trip_id,stop_id\nT1,S1\nT1,S2\nT2,S2\nT2,S3\n",
    "stops": (
        "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
        "S1,Gare 1,50.0,3.0,,P\n"
        "S2,Gare 2,50.1,3.1,0,\n"
        "S3,Arret 3,50.2,3.2,0,\n"
        "P,Station,50.0,3.0,1,\n"
    ),
    "calendar": (
        "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
        "WEEK,1,1,1,1,1,0,0,20260101,20261231\n"
        "SUN,0,0,0,0,0,0,1,20260101,20261231\n"
    ),
    # The weekly service is removed on 2026-03-11 and the Sunday service is added on that date.
    "calendar_dates": "service_id,date,exception_type\nWEEK,20260311,2\nSUN,20260311,1\n",
}


def test_read_gtfs_without_date(tmp_path):
    path = tmp_path / "gtfs.zip"
    write_gtfs(path, GTFS_TABLES)
    stops, routes = read_gtfs_stops_and_routes(path, None)
    stops = stops.sort("stop_id")
    # All stops are kept, including the parent station without route.
    assert stops["stop_id"].to_list() == ["P", "S1", "S2", "S3"]
    assert stops["route_ids"].to_list() == [[], ["R1"], ["R1", "R2"], ["R2"]]
    assert stops["location_type"].to_list() == ["station", "platform", "platform", "platform"]
    assert stops["parent_station"].to_list() == [None, "P", None, None]
    routes = routes.sort("route_id")
    assert routes["name"].to_list() == ["TER", "Bus 2"]
    assert routes["route_type"].to_list() == [2, 3]
    assert routes["color"].to_list() == ["#FF0000", None]
    # The agency id is filled when the GTFS has a single agency.
    assert routes["agency_id"].to_list() == ["A", "A"]


def test_read_gtfs_with_date(tmp_path):
    path = tmp_path / "gtfs.zip"
    write_gtfs(path, GTFS_TABLES)
    # Tuesday: weekly service only.
    stops, routes = read_gtfs_stops_and_routes(path, date(2026, 3, 10))
    assert sorted(stops["stop_id"]) == ["S1", "S2"]
    assert routes["route_id"].to_list() == ["R1"]
    # Wednesday with exceptions: Sunday service only.
    stops, routes = read_gtfs_stops_and_routes(path, date(2026, 3, 11))
    assert sorted(stops["stop_id"]) == ["S2", "S3"]
    assert routes["route_id"].to_list() == ["R2"]
