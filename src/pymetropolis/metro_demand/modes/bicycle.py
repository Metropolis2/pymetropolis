import polars as pl

from pymetropolis.metro_common.io import read_dataframe
from pymetropolis.metro_demand.modes.common import (
    ModePreferencesFromPopulationStep,
    PreferencesStep,
    cst_preferences_step_docstring,
    pref_constant_parameter,
    pref_file_parameter,
    pref_value_of_time_parameter,
    preferences_step_docstring,
)
from pymetropolis.metro_demand.routing.files import TripsPedestrianDistancesFile
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import FloatParameter

from .files import BicyclePreferencesFile, BicycleTravelTimesFile

MODE = "bicycle"


class BicyclePreferencesStep(PreferencesStep):
    __doc__ = cst_preferences_step_docstring(MODE)

    constant = pref_constant_parameter(MODE)
    value_of_time = pref_value_of_time_parameter(MODE)
    output_files = {"preferences": BicyclePreferencesFile}

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        df = self.get_preferences(MODE, persons)
        self.output["preferences"].write(df)


class BicyclePreferencesFromPopulationStep(ModePreferencesFromPopulationStep):
    __doc__ = preferences_step_docstring(MODE)

    pref_file = pref_file_parameter(MODE)
    output_files = {"preferences": BicyclePreferencesFile}

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        pref = read_dataframe(self.pref_file)
        df = self.get_person_preferences(persons, pref, MODE)
        self.output["preferences"].write(df)


class BicycleTravelTimesStep(Step):
    """Computes travel time by bicycle for each trip, from the pedestrian distance and a constant
    speed.
    """

    speed = FloatParameter(
        "modes.bicycle.speed", description="Constant bicycle speed for all trips, in km/h."
    )
    input_files = {"distances": TripsPedestrianDistancesFile}
    output_files = {"tts": BicycleTravelTimesFile}

    def is_defined(self):
        return self.speed is not None

    def run(self):
        distances = self.input["distances"].read()
        df = distances.select(
            "trip_id", bicycle_travel_time=self.speed * pl.col("pedestrian_distance") / 1000
        )
        self.output["tts"].write(df)
