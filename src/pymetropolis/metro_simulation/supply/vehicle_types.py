import polars as pl

from pymetropolis.metro_pipeline.parameters import FloatParameter
from pymetropolis.metro_simulation.common import StepWithModes

from .files import MetroVehicleTypesFile


class WriteMetroVehicleTypesStep(StepWithModes):
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
    output_files = {"metro_vehicle_types": MetroVehicleTypesFile}

    def is_defined(self):
        # The step does not need to be run if there is no "car" mode.
        return self.has_mode("car_driver") or self.has_mode("car_passenger")

    def run(self):
        df = pl.DataFrame(
            {"vehicle_id": ["car_driver"], "headway": [self.car_headway], "pce": [self.car_pce]}
        )
        self.output["metro_vehicle_types"].write(df)
