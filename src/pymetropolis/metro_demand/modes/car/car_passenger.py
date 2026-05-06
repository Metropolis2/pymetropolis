from __future__ import annotations

from typing import TYPE_CHECKING

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

from .files import CarPassengerPreferencesFile

if TYPE_CHECKING:
    import polars as pl

MODE = "car_passenger"


class CarPassengerPreferencesStep(PreferencesStep):
    __doc__ = cst_preferences_step_docstring(MODE)

    constant = pref_constant_parameter(MODE)
    value_of_time = pref_value_of_time_parameter(MODE)
    output_files = {"preferences": CarPassengerPreferencesFile}

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        df = self.get_preferences(MODE, persons)
        self.output["preferences"].write(df)


class CarPassengerPreferencesFromPopulationStep(ModePreferencesFromPopulationStep):
    __doc__ = preferences_step_docstring("car_passenger")

    pref_file = pref_file_parameter("car_passenger")
    output_files = {"preferences": CarPassengerPreferencesFile}

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        pref = read_dataframe(self.pref_file)
        df = self.get_person_preferences(persons, pref, "car_passenger")
        self.output["preferences"].write(df)
