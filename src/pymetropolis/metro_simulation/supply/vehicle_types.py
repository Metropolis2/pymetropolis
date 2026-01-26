import polars as pl

from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import FloatParameter

from .files import MetroVehicleTypesFile


class WriteMetroVehicleTypesStep(Step):
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
    output_files = {"metro_vehicle_types": MetroVehicleTypesFile}

    def run(self):
        df = pl.DataFrame(
            {"vehicle_id": ["car_driver"], "headway": [self.car_headway], "pce": [self.car_pce]}
        )
        self.output["metro_vehicle_types"].write(df)
