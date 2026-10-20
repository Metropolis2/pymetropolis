from pathlib import Path

import geopandas as gpd
import polars as pl
import pytest
from shapely.geometry import LineString

from pymetropolis.metro_network.road_network import RoadEdgesCleanFile
from pymetropolis.metro_pipeline import Config
from pymetropolis.metro_simulation.demand.files import MetroTripsFile
from pymetropolis.metro_simulation.supply.files import MetroVehicleTypesFile
from pymetropolis.metro_simulation.supply.vehicle_types import WriteMetroVehicleTypesStep
from pymetropolis.modes import (
    CAR_MODE_IDS,
    ROAD_MODE_IDS,
    ModeAvailabilityRules,
    Truck,
    mode_availability_expr,
    mode_from_str,
)


def test_truck_mode():
    assert mode_from_str("truck") is Truck
    assert "truck" in ROAD_MODE_IDS
    assert "truck" not in CAR_MODE_IDS
    assert Truck.vehicle().is_truck()
    assert not Truck.vehicle().is_car()


def test_truck_availability():
    # Trucks are available whatever the car ownership, driving license, age or joint tour.
    df = pl.DataFrame(
        {
            "nb_cars": [1, 0, None],
            "has_driving_license": [True, False, None],
            "age": [30, 10, None],
            "joint_tour": [False, True, None],
        }
    )
    rules = ModeAvailabilityRules(car_driver_min_age=18)
    out = df.with_columns(mode_availability_expr(Truck, rules, df.columns).alias("avail"))
    assert out["avail"].to_list() == [True, True, True]


def write_inputs(main_directory: Path, vehicles: list[str]):
    edges = gpd.GeoDataFrame(
        {"edge_id": [1], "hov_lanes": [0]},
        geometry=[LineString([(0, 0), (1000, 0)])],
        crs="EPSG:2154",
    )
    edges_path = RoadEdgesCleanFile.from_dir(main_directory).get_path()
    edges_path.parent.mkdir(parents=True, exist_ok=True)
    edges.to_parquet(edges_path)
    trips_path = MetroTripsFile.from_dir(main_directory).get_path()
    trips_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"class.vehicle": vehicles}).write_parquet(trips_path)


def run_vehicle_types(main_directory: Path, config: dict) -> pl.DataFrame:
    WriteMetroVehicleTypesStep(Config({"main_directory": str(main_directory), **config})).run()
    return MetroVehicleTypesFile.from_dir(main_directory).read().sort("vehicle_id")


def test_truck_vehicle_type_defaults(tmp_path: Path):
    write_inputs(tmp_path, ["car_driver_alone", "truck"])
    df = run_vehicle_types(tmp_path, {"simulation_ratio": 0.5})
    assert df["vehicle_id"].to_list() == ["car_driver_alone", "truck"]
    # Headway and PCE are scaled by the simulation ratio; trucks are 3 times a car by default.
    assert df["headway"].to_list() == pytest.approx([16.0, 48.0])
    assert df["pce"].to_list() == pytest.approx([2.0, 6.0])
    # No speed limit by default.
    assert "speed_function.type" not in df.columns


def test_truck_vehicle_type_custom(tmp_path: Path):
    write_inputs(tmp_path, ["truck"])
    config = {"vehicle_types": {"truck": {"headway": 15.0, "pce": 2.5, "max_speed": 90.0}}}
    df = run_vehicle_types(tmp_path, config)
    assert df["headway"].to_list() == pytest.approx([15.0])
    assert df["pce"].to_list() == pytest.approx([2.5])
    # A maximum speed of 90 km/h.
    assert df["speed_function.type"].to_list() == ["UpperBound"]
    assert df["speed_function.upper_bound"].to_list() == pytest.approx([90.0 / 3.6])
