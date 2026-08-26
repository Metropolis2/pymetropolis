from typing import TYPE_CHECKING

from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_network.road_network import RoadEdgesCleanFile
from pymetropolis.metro_pipeline.parameters import FloatParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_simulation.common import StepWithRidesharingCount, StepWithSimulationRatio
from pymetropolis.metro_simulation.demand.files import MetroTripsFile

from .files import MetroVehicleTypesFile

if TYPE_CHECKING:
    import geopandas as gpd


class WriteMetroVehicleTypesStep(StepWithRidesharingCount, StepWithSimulationRatio):
    """Generates the input vehicle-types file for the Metropolis-Core simulation."""

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
    input_files = {
        "edges": InputFile(RoadEdgesCleanFile, optional=True),
        "metro_trips": MetroTripsFile,
    }
    output_files = {"metro_vehicle_types": MetroVehicleTypesFile}

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
            # Note. This is an actual issue only if "car_driver_alone" is simulated, but it's best
            # to raise an error in any case.
            raise MetropyError("Cannot write vehicle types when there is no road network.")

        metro_vehicles = list()
        headway = self.car_headway / self.simulation_ratio
        pce = self.car_pce / self.simulation_ratio
        if "car_driver_alone" in vehicles:
            v = {"vehicle_id": "car_driver_alone", "headway": headway, "pce": pce}
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
            if hov_edges:
                v["restricted_edges"] = hov_edges
            metro_vehicles.append(v)
        if "car_driver_multi" in vehicles:
            metro_vehicles.append(
                {"vehicle_id": "car_driver_multi", "headway": headway, "pce": pce}
            )
        if "car_passenger" in vehicles:
            metro_vehicles.append({"vehicle_id": "car_passenger", "headway": 0.0, "pce": 0.0})
        if "car_ridesharing" in vehicles:
            c = self.ridesharing_passenger_count
            if c < 0.0:
                raise MetropyError(
                    "Number of passenger count for ridesharing is negative "
                    f"(`ridesharing_passenger_count` = {c})"
                )
            metro_vehicles.append(
                {
                    "vehicle_id": "car_ridesharing",
                    "headway": headway / (c + 1),
                    "pce": pce / (c + 1),
                }
            )
        df = pl.DataFrame(metro_vehicles)
        self.output["metro_vehicle_types"].write(df)
