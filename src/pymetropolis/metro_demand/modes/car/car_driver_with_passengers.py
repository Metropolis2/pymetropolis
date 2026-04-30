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

from .files import CarDriverWithPassengersPreferencesFile

MODE = "car_driver_with_passengers"


class CarDriverWithPassengersPreferencesStep(PreferencesStep):
    __doc__ = cst_preferences_step_docstring(MODE)

    constant = pref_constant_parameter(MODE)
    value_of_time = pref_value_of_time_parameter(MODE)
    output_files = {"preferences": CarDriverWithPassengersPreferencesFile}

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        df = self.get_preferences(MODE, persons)
        self.output["preferences"].write(df)


class CarDriverWithPassengersPreferencesFromPopulationStep(ModePreferencesFromPopulationStep):
    __doc__ = preferences_step_docstring("car_driver_with_passengers")

    pref_file = pref_file_parameter("car_driver_with_passengers")
    output_files = {"preferences": CarDriverWithPassengersPreferencesFile}

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        pref = read_dataframe(self.pref_file)
        df = self.get_person_preferences(persons, pref, "car_driver_with_passengers")
        self.output["preferences"].write(df)
