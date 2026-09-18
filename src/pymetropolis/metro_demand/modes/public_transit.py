from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_demand.modes.common import (
    ModePreferencesFromPopulationStep,
    PreferencesStep,
    cst_preferences_step_docstring,
    pref_constant_parameter,
    pref_file_parameter,
    pref_value_of_time_parameter,
    preferences_step_docstring,
)
from pymetropolis.metro_demand.routing.files import (
    TripsCarFreeFlowTravelTimesFile,
    TripsPublicTransitItinerariesFile,
)
from pymetropolis.metro_pipeline import PopulationStep
from pymetropolis.metro_pipeline.parameters import FloatParameter

from .files import PublicTransitPreferencesFile

if TYPE_CHECKING:
    import polars as pl

MODE = "public_transit"


class PublicTransitPreferencesStep(PreferencesStep, PopulationStep):
    __doc__ = cst_preferences_step_docstring(MODE)

    _mode = MODE

    constant = pref_constant_parameter(MODE)
    value_of_time = pref_value_of_time_parameter(MODE)
    output_files = {"preferences": PublicTransitPreferencesFile}


class PublicTransitPreferencesFromPopulationStep(ModePreferencesFromPopulationStep):
    __doc__ = preferences_step_docstring(MODE)

    _mode = MODE

    pref_file = pref_file_parameter(MODE)
    output_files = {"preferences": PublicTransitPreferencesFile}


class PublicTransitTravelTimesFromRoadDistancesStep(PopulationStep):
    """Generates travel times for the public-transit trips by applying a constant speed to
    the shortest-path distances of the car-driver trips.
    """

    speed = FloatParameter(
        "modes.public_transit.road_network_speed",
        description="Speed of public-transit vehicles on the road network (km/h).",
    )
    input_files = {"car_driver_distances": TripsCarFreeFlowTravelTimesFile}
    output_files = {"public_transit_travel_times": TripsPublicTransitItinerariesFile}

    def is_defined(self) -> bool:
        return self.speed is not None

    def run(self):
        import polars as pl

        df: pl.DataFrame = self.input["car_driver_distances"].read()
        df = df.select(
            "trip_id",
            travel_time=pl.duration(seconds=pl.col("free_flow_distance") / self.speed * 3.6),
        )
        self.output["public_transit_travel_times"].write(df)
