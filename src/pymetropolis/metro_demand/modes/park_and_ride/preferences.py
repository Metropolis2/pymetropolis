from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_common.io import read_dataframe
from pymetropolis.metro_demand.modes.car.files import CarDriverPreferencesFile
from pymetropolis.metro_demand.modes.common import (
    ModePreferencesFromPopulationStep,
    PreferencesStep,
    pref_constant_parameter,
    pref_file_parameter,
)
from pymetropolis.metro_demand.modes.files import PublicTransitPreferencesFile
from pymetropolis.metro_demand.population.files import ToursFile
from pymetropolis.metro_pipeline.steps import InputFile

from .files import ParkAndRidePreferencesFile

if TYPE_CHECKING:
    import polars as pl

    from pymetropolis.metro_pipeline.file import MetroFile

MODE = "park_and_ride"

VOT_NOTE = """
    The values of time of the car part and of the public-transit part are not defined here: they
    are read from the preferences of the `car_driver` mode
    ([`CarDriverPreferencesFile`](files.md#cardriverpreferencesfile)) and of the `public_transit`
    mode ([`PublicTransitPreferencesFile`](files.md#publictransitpreferencesfile)), respectively.
    When these files do not exist, the corresponding value of time is 0.
"""


def add_values_of_time(df: pl.DataFrame, pt_prefs: MetroFile, car_prefs: MetroFile) -> pl.DataFrame:
    """Adds the `public_transit_vot` and `car_vot` columns to the P+R preferences, from the
    preferences of the `public_transit` and `car_driver` modes.
    """
    import polars as pl

    for prefs, col, out_col in (
        (pt_prefs, "public_transit_vot", "public_transit_vot"),
        (car_prefs, "car_driver_vot", "car_vot"),
    ):
        if prefs.exists():
            df = df.join(
                prefs.read().select("tour_id", pl.col(col).alias(out_col)), on="tour_id", how="left"
            )
        else:
            df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(out_col))
    return df.with_columns(pl.col("public_transit_vot", "car_vot").fill_null(0.0))


class ParkAndRidePreferencesStep(PreferencesStep):
    __doc__ = (
        """Generates the preference parameters of traveling by park-and-ride, for each tour, from
    exogenous values.

    The constant (penalty of traveling by park-and-ride, *per tour*) can be constant over tours or
    sampled from a specific distribution.
    """
        + VOT_NOTE
    )

    _mode = MODE

    constant = pref_constant_parameter(MODE)
    input_files = {
        "tours": ToursFile,
        "pt_prefs": InputFile(PublicTransitPreferencesFile, optional=True),
        "car_prefs": InputFile(CarDriverPreferencesFile, optional=True),
    }
    output_files = {"preferences": ParkAndRidePreferencesFile}

    def is_defined(self):
        return self.has_mode(MODE)

    def run(self):
        tours: pl.DataFrame = self.input["tours"].read()
        df = self.get_preferences(tours).drop(f"{MODE}_vot")
        df = add_values_of_time(df, self.input["pt_prefs"], self.input["car_prefs"])
        self.output["preferences"].write(df)


class ParkAndRidePreferencesFromPopulationStep(ModePreferencesFromPopulationStep):
    __doc__ = (
        """Generates the preference parameters of traveling by park-and-ride, for each tour, from
    constant values over population segments.

    The [`modes.park_and_ride.preferences_file`](parameters.md#modespark_and_ridepreferences_file)
    parameter must point to a Parquet or CSV file with the `constant` value for the population
    segments (see
    [`CarDriverPreferencesFromPopulationStep`](steps.md#cardriverpreferencesfrompopulationstep) for
    the file format). Value-of-time columns are ignored.

    This Step takes precedence over
    [`ParkAndRidePreferencesStep`](steps.md#parkandridepreferencesstep).
    """
        + VOT_NOTE
    )

    _mode = MODE
    # Higher priority than `ParkAndRidePreferencesStep` (which is always defined).
    priority = 2

    pref_file = pref_file_parameter(MODE)
    input_files = {
        "tours": ToursFile,
        "pt_prefs": InputFile(PublicTransitPreferencesFile, optional=True),
        "car_prefs": InputFile(CarDriverPreferencesFile, optional=True),
    }
    output_files = {"preferences": ParkAndRidePreferencesFile}

    def run(self):
        assert self.pref_file is not None
        tours: pl.DataFrame = self.input["tours"].read()
        pref = read_dataframe(self.pref_file).drop(["alpha", "value_of_time"], strict=False)
        df = self.get_tour_preferences(tours, pref)
        df = add_values_of_time(df, self.input["pt_prefs"], self.input["car_prefs"])
        self.output["preferences"].write(df)
