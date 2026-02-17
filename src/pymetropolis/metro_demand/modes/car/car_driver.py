import polars as pl

from pymetropolis.metro_demand.population import PersonsFile
from pymetropolis.random import FloatDistributionParameter, RandomStep, generate_values

from .files import CarDriverPreferencesFile


class CarDriverPreferencesStep(RandomStep):
    """Generates the preference parameters of traveling as a car driver, for each trip, from
    exogenous values.

    The following parameters are generated:

    - constant: penalty of traveling as car driver, *per trip*
    - value of time / alpha: penalty per hour spent traveling as a car driver

    The values can be constant over trips or sampled from a specific distribution.
    """

    constant = FloatDistributionParameter(
        "modes.car_driver.constant",
        default=0.0,
        description="Constant penalty for each trip as a car driver (€).",
    )
    value_of_time = FloatDistributionParameter(
        "modes.car_driver.alpha",
        default=0.0,
        description="Value of time as a car driver (€/h).",
    )
    input_files = {"persons": PersonsFile}
    output_files = {"preferences": CarDriverPreferencesFile}

    def is_defined(self):
        return self.constant != 0.0 or self.value_of_time != 0.0

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        rng = self.get_rng()
        df = persons.select(
            "person_id",
            car_driver_cst=generate_values(self.constant, len(persons), rng),
            car_driver_vot=generate_values(self.value_of_time, len(persons), rng),
        )
        self.output["preferences"].write(df)
