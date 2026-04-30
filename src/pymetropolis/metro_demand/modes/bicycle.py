import polars as pl

from pymetropolis.metro_common.io import read_dataframe
from pymetropolis.metro_demand.modes.common import (
    ModePreferencesFromPopulationStep,
    pref_file_parameter,
    preferences_step_docstring,
)
from pymetropolis.metro_demand.population import PersonsFile
from pymetropolis.random import FloatDistributionParameter, RandomStep, generate_values

from .files import BicyclePreferencesFile


class BicyclePreferencesStep(RandomStep):
    """Generates the preference parameters of traveling by bicycle, for each trip, from exogenous
    values.

    The following parameters are generated:

    - constant: penalty of traveling by bicycle, *per trip*
    - value of time / alpha: penalty per hour spent traveling by bicycle

    The values can be constant over trips or sampled from a specific distribution.
    """

    constant = FloatDistributionParameter(
        "modes.bicycle.constant",
        default=0.0,
        description="Constant penalty for each bicycle trip (€).",
    )
    value_of_time = FloatDistributionParameter(
        "modes.bicycle.alpha", default=0.0, description="Value of time by bicycle (€/h)."
    )
    input_files = {"persons": PersonsFile}
    output_files = {"bicycle_preferences": BicyclePreferencesFile}

    def is_defined(self):
        return self.constant != 0.0 or self.value_of_time != 0.0

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        rng = self.get_rng()
        df = persons.select(
            "person_id",
            bicycle_cst=generate_values(self.constant, len(persons), rng),
            bicycle_vot=generate_values(self.value_of_time, len(persons), rng),
        )
        self.output["bicycle_preferences"].write(df)


class BicyclePreferencesFromPopulationStep(ModePreferencesFromPopulationStep):
    __doc__ = preferences_step_docstring("bicycle")

    pref_file = pref_file_parameter("bicycle")
    output_files = {"bicycle_preferences": BicyclePreferencesFile}

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        pref = read_dataframe(self.pref_file)
        df = self.get_person_preferences(persons, pref, "bicycle")
        self.output["bicycle_preferences"].write(df)
