import geopandas as gpd
import polars as pl

from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_network.road_network import RoadEdgesCleanFile
from pymetropolis.metro_pipeline.parameters import FloatParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_simulation.common import StepWithModes, StepWithRidesharingCount

from .files import MetroVehicleTypesFile


class WriteMetroVehicleTypesStep(StepWithModes, StepWithRidesharingCount):
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
        "edges": InputFile(
            RoadEdgesCleanFile,
            when=lambda inst: inst.has_mode("car_driver"),
            when_doc='if the "car_driver" mode is defined',
        )
    }
    output_files = {"metro_vehicle_types": MetroVehicleTypesFile}

    def is_defined(self):
        # The step does not need to be run if there is no "car" mode.
        return self.has_car_mode()

    def run(self):
        vehicles = list()
        if self.has_mode("car_driver"):
            v = {"vehicle_id": "car_driver_alone", "headway": self.car_headway, "pce": self.car_pce}
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
            vehicles.append(v)
        if self.has_mode("car_driver_with_passengers"):
            vehicles.append(
                {"vehicle_id": "car_driver_multi", "headway": self.car_headway, "pce": self.car_pce}
            )
        if self.has_mode("car_passenger"):
            vehicles.append({"vehicle_id": "car_passenger", "headway": 0.0, "pce": 0.0})
        if self.has_mode("car_ridesharing"):
            c = self.ridesharing_passenger_count
            if c < 0.0:
                raise MetropyError(
                    "Number of passenger count for ridesharing is negative "
                    f"(`ridesharing_passenger_count` = {c})"
                )
            vehicles.append(
                {
                    "vehicle_id": "car_ridesharing",
                    "headway": self.car_headway / (c + 1),
                    "pce": self.car_pce / (c + 1),
                }
            )
        df = pl.DataFrame(vehicles)
        self.output["metro_vehicle_types"].write(df)
