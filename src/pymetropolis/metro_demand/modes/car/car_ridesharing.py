from pymetropolis.metro_demand.modes.common import (
    ModePreferencesFromPopulationStep,
    PreferencesStep,
    cst_preferences_step_docstring,
    pref_constant_parameter,
    pref_file_parameter,
    pref_value_of_time_parameter,
    preferences_step_docstring,
)
from pymetropolis.metro_pipeline import PopulationStep

from .files import CarRidesharingPreferencesFile

MODE = "car_ridesharing"


class CarRidesharingPreferencesStep(PreferencesStep, PopulationStep):
    __doc__ = cst_preferences_step_docstring(MODE)

    _mode = MODE

    constant = pref_constant_parameter(MODE)
    value_of_time = pref_value_of_time_parameter(MODE)
    output_files = {"preferences": CarRidesharingPreferencesFile}


class CarRidesharingPreferencesFromPopulationStep(ModePreferencesFromPopulationStep):
    __doc__ = preferences_step_docstring(MODE)

    _mode = MODE

    pref_file = pref_file_parameter(MODE)
    output_files = {"preferences": CarRidesharingPreferencesFile}
