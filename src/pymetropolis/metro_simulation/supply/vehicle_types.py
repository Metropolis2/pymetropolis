from typing import TYPE_CHECKING

from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_network.road_network import RoadEdgesCleanFile
from pymetropolis.metro_pipeline.parameters import FloatParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_simulation.common import StepWithRidesharingCount, StepWithSimulationRatio
from pymetropolis.metro_simulation.demand.files import MetroExAnteTripsFile, MetroTripsFile
from pymetropolis.modes import METRO_VEHICLES

from .files import MetroExAnteVehicleTypesFile, MetroVehicleTypesFile

if TYPE_CHECKING:
    import geopandas as gpd


class AbstractWriteMetroVehicleTypesStep(StepWithRidesharingCount, StepWithSimulationRatio):
    """Abstract Step to generate an input vehicle-types file for Metropolis-Core.

    The vehicle types to write are read from the `class.vehicle` column of the simulated trips, so
    that only the vehicle types actually used are defined.
    """

    car_headway = FloatParameter(
        "vehicle_types.car.headway",
        default=8.0,
        description="Typical length between two cars, from head to head, in meters",
    )
    car_pce = FloatParameter(
        "vehicle_types.car.pce",
        default=1.0,
        description="Passenger car equivalent of a typical car",
    )
    truck_headway = FloatParameter(
        "vehicle_types.truck.headway",
        default=20.0,
        description="Typical length between two trucks, from head to head, in meters",
    )
    truck_pce = FloatParameter(
        "vehicle_types.truck.pce",
        default=2.0,
        description="Passenger car equivalent of a typical truck",
    )
    truck_max_speed = FloatParameter(
        "vehicle_types.truck.max_speed",
        default=90.0,
        description="Maximum speed of trucks (km/h)",
        note="Set to 0 to disable the speed limitation (trucks drive at the edges' speed).",
        lower_bound=0.0,
    )

    def run(self):
        import polars as pl

        assert self.car_headway is not None
        assert self.car_pce is not None
        assert self.truck_headway is not None
        assert self.truck_pce is not None
        assert self.truck_max_speed is not None
        assert self.simulation_ratio is not None
        assert self.ridesharing_passenger_count is not None

        trips = self.input["metro_trips"].read()
        vehicles = set(trips["class.vehicle"].unique().drop_nulls().sort())

        if not vehicles:
            # No vehicle-based trip to simulate.
            return

        if not self.input["edges"].exists():
            # At this point, vehicle-based trips were defined but there is no road network.
            # This should never happen in practice.
            raise MetropyError("Cannot write vehicle types when there is no road network.")

        # TODO. Maybe `hov_lanes` should be a column in its own file?
        edges_gdf: gpd.GeoDataFrame = self.input["edges"].read()
        edges = pl.from_pandas(edges_gdf.loc[:, ["edge_id", "hov_lanes"]])
        hov_edges = (
            edges.filter(pl.col("hov_lanes") > 0)
            .select(pl.concat_str(pl.col("edge_id").cast(pl.String), pl.lit("-hov")))
            .to_series()
            .sort()
            .to_list()
        )

        metro_vehicles = list()
        car_headway = self.car_headway / self.simulation_ratio
        car_pce = self.car_pce / self.simulation_ratio
        for vehicle in METRO_VEHICLES:
            if repr(vehicle) not in vehicles:
                continue
            congestion_share = vehicle.get_congestion_share(self.ridesharing_passenger_count)
            v: dict = {"vehicle_id": repr(vehicle)}
            if vehicle.is_car():
                v["headway"] = car_headway * congestion_share
                v["pce"] = car_pce * congestion_share
            elif vehicle.is_truck():
                v["headway"] = self.truck_headway / self.simulation_ratio * congestion_share
                v["pce"] = self.truck_pce / self.simulation_ratio * congestion_share
                if self.truck_max_speed > 0.0:
                    v["speed_function.type"] = "UpperBound"
                    v["speed_function.upper_bound"] = self.truck_max_speed / 3.6
            else:
                raise MetropyError(f"Vehicle {vehicle} is not a car nor a truck.")
            if hov_edges and not vehicle.can_take_hov_edges():
                v["restricted_edges"] = hov_edges
            metro_vehicles.append(v)
        df = pl.DataFrame(metro_vehicles, infer_schema_length=None)
        self.output["metro_vehicle_types"].write(df)


class WriteMetroVehicleTypesStep(AbstractWriteMetroVehicleTypesStep):
    """Generates the input vehicle-types file for the Metropolis-Core simulation."""

    input_files = {
        "edges": InputFile(RoadEdgesCleanFile, optional=True),
        "metro_trips": MetroTripsFile,
    }
    output_files = {"metro_vehicle_types": MetroVehicleTypesFile}


class WriteExAnteMetroVehicleTypesStep(AbstractWriteMetroVehicleTypesStep):
    """Generates the input vehicle-types file for the ex-ante simulation."""

    input_files = {
        "edges": InputFile(RoadEdgesCleanFile, optional=True),
        "metro_trips": MetroExAnteTripsFile,
    }
    output_files = {"metro_vehicle_types": MetroExAnteVehicleTypesFile}
    priority = 0
