from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_common.io import read_dataframe
from pymetropolis.metro_demand.modes.car.files import CarDriverPreferencesFile
from pymetropolis.metro_demand.modes.common import (
    ModePreferencesFromPopulationStep,
    PreferencesStep,
    cst_preferences_step_docstring,
    pref_constant_parameter,
    pref_file_parameter,
    pref_value_of_time_parameter,
    preferences_step_docstring,
)
from pymetropolis.metro_demand.modes.files import PublicTransitPreferencesFile

from .files import ParkAndRidePreferencesFile

if TYPE_CHECKING:
    import polars as pl

MODE = "park_and_ride"


# PFR. Here I just put the 2 generic steps to generate cst and VOT by mode but I think it's not
# adapted for park-and-ride (with 3 params, two of which should be read from PT and car files
# probably).
# Let's discuss that together to find the best way to define preferences.
class ParkAndRidePreferencesStep(PreferencesStep):
    __doc__ = cst_preferences_step_docstring(MODE)

    constant = pref_constant_parameter(MODE)
    value_of_time = pref_value_of_time_parameter(MODE)
    input_files = {"pt_prefs": PublicTransitPreferencesFile, "car_prefs": CarDriverPreferencesFile}
    output_files = {"preferences": ParkAndRidePreferencesFile}

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        df = self.get_preferences(MODE, persons)
        self.output["preferences"].write(df)


class ParkAndRidePreferencesFromPopulationStep(ModePreferencesFromPopulationStep):
    __doc__ = preferences_step_docstring(MODE)

    pref_file = pref_file_parameter(MODE)
    input_files = {"pt_prefs": PublicTransitPreferencesFile, "car_prefs": CarDriverPreferencesFile}
    output_files = {"preferences": ParkAndRidePreferencesFile}

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        pref = read_dataframe(self.pref_file)
        df = self.get_person_preferences(persons, pref, MODE)
        self.output["preferences"].write(df)
