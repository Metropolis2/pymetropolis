import polars as pl

from pymetropolis.metro_demand.population import PersonsFile
from pymetropolis.random import FloatDistributionParameter, RandomStep, generate_values

from .files import WalkingPreferencesFile


class WalkingPreferencesStep(RandomStep):
    """Generates the preference parameters of traveling by walk, for each trip, from exogenous
    values.

    The following parameters are generated:

    - constant: penalty of traveling by walk, *per trip*
    - value of time / alpha: penalty per hour spent traveling by walk

    The values can be constant over trips or sampled from a specific distribution.
    """

    constant = FloatDistributionParameter(
        "modes.walking.constant",
        default=0.0,
        description="Constant penalty for each walking trip (€).",
    )
    value_of_time = FloatDistributionParameter(
        "modes.walking.alpha", default=0.0, description="Value of time by walk (€/h)."
    )
    input_files = {"persons": PersonsFile}
    output_files = {"walking_preferences": WalkingPreferencesFile}

    def is_defined(self):
        return self.constant != 0.0 or self.value_of_time != 0.0

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        rng = self.get_rng()
        df = persons.select(
            "person_id",
            walking_cst=generate_values(self.constant, len(persons), rng),
            walking_vot=generate_values(self.value_of_time, len(persons), rng),
        )
        self.output["walking_preferences"].write(df)
