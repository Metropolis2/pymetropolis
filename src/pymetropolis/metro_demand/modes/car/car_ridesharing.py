import polars as pl

from pymetropolis.metro_demand.population import PersonsFile
from pymetropolis.random import FloatDistributionParameter, RandomStep, generate_values

from .files import CarRidesharingPreferencesFile


class CarRidesharingPreferencesStep(RandomStep):
    """Generates the preference parameters of traveling by car ridesharing, for each trip, from
    exogenous values.

    The following parameters are generated:

    - constant: penalty of traveling by car ridesharing, *per trip*
    - value of time / alpha: penalty per hour spent traveling by car ridesharing
    - passenger_count: number of passengers in the car (excluding the driver); it is used to define
      how fuel costs are shared and how congestion is computed

    The values can be constant over trips or sampled from a specific distribution.
    """

    constant = FloatDistributionParameter(
        "modes.car_ridesharing.constant",
        default=0.0,
        description="Constant penalty for each trip by car ridesharing (€).",
    )
    value_of_time = FloatDistributionParameter(
        "modes.car_ridesharing.alpha",
        default=0.0,
        description="Value of time by car ridesharing (€/h).",
    )
    input_files = {"persons": PersonsFile}
    output_files = {"preferences": CarRidesharingPreferencesFile}

    def is_defined(self):
        return self.constant != 0.0 or self.value_of_time != 0.0

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        rng = self.get_rng()
        df = persons.select(
            "person_id",
            car_ridesharing_cst=generate_values(self.constant, len(persons), rng),
            car_ridesharing_vot=generate_values(self.value_of_time, len(persons), rng),
        )
        self.output["preferences"].write(df)
