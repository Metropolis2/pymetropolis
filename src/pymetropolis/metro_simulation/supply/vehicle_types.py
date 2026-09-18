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

    def run(self):
        import polars as pl

        assert self.car_headway is not None
        assert self.car_pce is not None
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
            if vehicle.is_car():
                headway = car_headway * congestion_share
                pce = car_pce * congestion_share
            else:
                raise MetropyError(f"Vehicle {vehicle} is not a car.")
            v = {"vehicle_id": repr(vehicle), "headway": headway, "pce": pce}
            if hov_edges and not vehicle.can_take_hov_edges():
                v["restricted_edges"] = hov_edges
            metro_vehicles.append(v)
        df = pl.DataFrame(metro_vehicles)
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
