from __future__ import annotations

from pymetropolis.metro_demand.modes.common import (
    ModePreferencesFromPopulationStep,
    PreferencesStep,
    cst_preferences_step_docstring,
    pref_constant_parameter,
    pref_file_parameter,
    pref_value_of_time_parameter,
    preferences_step_docstring,
)

from .files import ParkAndRidePreferencesFile

MODE = "park_and_ride"


# PFR. Here I just put the 2 generic steps to generate cst and VOT by mode but I think it's not
# adapted for park-and-ride (with 3 params, two of which should be read from PT and car files
# probably).
# Let's discuss that together to find the best way to define preferences.
class ParkAndRidePreferencesStep(PreferencesStep):
    __doc__ = cst_preferences_step_docstring(MODE)

    _mode = MODE

    constant = pref_constant_parameter(MODE)
    value_of_time = pref_value_of_time_parameter(MODE)
    # input_files = {"pt_prefs": PublicTransitPreferencesFile, "car_prefs": CarDriverPreferencesFile}
    output_files = {"preferences": ParkAndRidePreferencesFile}


class ParkAndRidePreferencesFromPopulationStep(ModePreferencesFromPopulationStep):
    __doc__ = preferences_step_docstring(MODE)

    _mode = MODE

    pref_file = pref_file_parameter(MODE)
    # input_files = {"pt_prefs": PublicTransitPreferencesFile, "car_prefs": CarDriverPreferencesFile}
    output_files = {"preferences": ParkAndRidePreferencesFile}
