from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_common import MetropyError
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
from pymetropolis.metro_demand.routing.files import (
    TripsPedestrianDistancesFile,
    TripsPedestrianNodesFile,
)
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import BoolParameter, EnumParameter, FloatParameter
from pymetropolis.metro_pipeline.steps import InputFile

from .files import BicyclePreferencesFile, BicycleTravelTimesFile

if TYPE_CHECKING:
    import polars as pl

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


class BicycleTravelTimesFromDistanceStep(Step):
    """Computes travel time by bicycle for each trip, from a given distance and a constant speed.

    The parameter [`modes.bicycle.distance.type`](parameters.md#modesbicycledistancetype) specifies
    how the bicycle distance is computed. For now, the only option is `"pedestrian"` which uses the
    distance of the shortest path on the pedestrian network (Step
    [`TripsPedestrianDistancesStep`](steps.md#TripsPedestrianDistancesStep)).

    The parameter [`modes.bicycle.speed`](parameters.md#modesbicyclespeed) controls the speed at
    which bicycles run.

    If the parameter
    [`modes.bicycle.distance.with_snap`](parameters.md#modesbicycledistancewith_snap) is set to
    `true`, the snap distance at origin and destination is added to the bicycle travel time, with a
    speed given by [`modes.bicycle.snap_speed`](parameters.md#modesbicyclesnap_speed) (equal to
    [`modes.bicycle.speed`](parameters.md#modesbicyclesspeed) by default).
    The snap distance corresponds to the distance between the trips' origin and destination and the
    network (See Step
    [`PedestrianODNodesFromCoordinatesStep`](steps.md#pedestrianodnodesfromcoordinatesstep)).
    """

    distance_type = EnumParameter(
        "modes.bicycle.distance.type",
        values=["pedestrian"],
        description="How distance of bicycle trips is computed.",
    )
    speed = FloatParameter(
        "modes.bicycle.speed", description="Constant bicycle speed for all trips, in km/h."
    )
    with_snap = BoolParameter(
        "modes.bicycle.distance.with_snap",
        default=False,
        description=(
            "Whether snap distances at origin and destination should be added to the trips' total "
            "distances.",
        ),
    )
    snap_speed = FloatParameter(
        "modes.bicycle.snap_speed",
        description="Bicycle speed on the snap part of the trip, in km/h.",
        note="Default is to use `modes.bicycle.speed` as snap speed.",
    )
    input_files = {
        "distances": InputFile(
            TripsPedestrianDistancesFile,
            when=lambda inst: inst.distance_type == "pedestrian",
            when_doc='`modes.bicycle.distance.type` is `"pedestrian"`',
        ),
        "snap_distances": InputFile(
            TripsPedestrianNodesFile,
            when=lambda inst: inst.with_snap,
            when_doc="`modes.bicycle.distance.with_snap` is `true`",
        ),
    }
    output_files = {"tts": BicycleTravelTimesFile}

    def is_defined(self):
        return self.distance_type is not None and self.speed is not None

    def run(self):
        import polars as pl

        if self.distance_type == "pedestrian":
            distances = self.input["distances"].read()
            distances = distances.select("trip_id", distance=pl.col("pedestrian_distance"))
        else:
            raise MetropyError(f"Unknown distance type: {self.distance_type}")
        df = distances.select(
            "trip_id",
            bicycle_travel_time=pl.duration(
                seconds=3600 * (pl.col("distance") / 1000) / self.speed
            ),
        )
        if self.with_snap:
            snap_speed = self.snap_speed or self.speed
            snap_distances = self.input["snap_distances"].read()
            snap_distances = snap_distances.select(
                "trip_id",
                snap_distance_km=(
                    pl.col("origin_pedestrian_edge_dist")
                    + pl.col("destination_pedestrian_edge_dist")
                )
                / 1000,
            )
            snap_distances = snap_distances.select(
                "trip_id",
                snap_travel_time=pl.duration(
                    seconds=3600 * pl.col("snap_distance_km") / snap_speed
                ),
            )
            df = df.join(snap_distances, on="trip_id", how="left").select(
                "trip_id",
                bicycle_travel_time=pl.col("bicycle_travel_time") + pl.col("snap_travel_time"),
            )
        self.output["tts"].write(df)
