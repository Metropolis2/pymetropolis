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

from .files import WalkingPreferencesFile, WalkingTravelTimesFile

if TYPE_CHECKING:
    import polars as pl

MODE = "walking"


class WalkingPreferencesStep(PreferencesStep):
    __doc__ = cst_preferences_step_docstring(MODE)

    constant = pref_constant_parameter(MODE)
    value_of_time = pref_value_of_time_parameter(MODE)
    output_files = {"preferences": WalkingPreferencesFile}

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        df = self.get_preferences(MODE, persons)
        self.output["preferences"].write(df)


class WalkingPreferencesFromPopulationStep(ModePreferencesFromPopulationStep):
    __doc__ = preferences_step_docstring("walking")

    pref_file = pref_file_parameter("walking")
    output_files = {"preferences": WalkingPreferencesFile}

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        pref = read_dataframe(self.pref_file)
        df = self.get_person_preferences(persons, pref, "walking")
        self.output["preferences"].write(df)


class WalkingTravelTimesFromDistanceStep(Step):
    """Computes travel time by walking for each trip, from a given distance and a constant speed.

    The parameter [`modes.walking.distance.type`](parameters.md#modeswalkingdistancetype) specifies
    how the walking distance is computed. For now, the only option is `"pedestrian"` which uses the
    distance of the shortest path on the pedestrian network (Step
    [`TripsPedestrianDistancesStep`](steps.md#TripsPedestrianDistancesStep)).

    The parameter [`modes.walking.speed`](parameters.md#modeswalkingspeed) controls the speed at
    which people walk.

    If the parameter
    [`modes.walking.distance.with_snap`](parameters.md#modeswalkingdistancewith_snap) is set to
    `true`, the snap distance at origin and destination is added to the walking travel time, with a
    speed given by [`modes.walking.snap_speed`](parameters.md#modeswalkingsnap_speed) (equal to
    [`modes.walking.speed`](parameters.md#modeswalkingsspeed) by default).
    The snap distance corresponds to the distance between the trips' origin and destination and the
    network (See Step
    [`PedestrianODNodesFromCoordinatesStep`](steps.md#pedestrianodnodesfromcoordinatesstep)).
    """

    distance_type = EnumParameter(
        "modes.walking.distance.type",
        values=["pedestrian"],
        description="How distance of walking trips is computed.",
    )
    speed = FloatParameter(
        "modes.walking.speed", description="Constant walking speed for all trips, in km/h."
    )
    with_snap = BoolParameter(
        "modes.walking.distance.with_snap",
        default=False,
        description=(
            "Whether snap distances at origin and destination should be added to the trips' total "
            "distances.",
        ),
    )
    snap_speed = FloatParameter(
        "modes.walking.snap_speed",
        description="Walking speed on the snap part of the trip, in km/h.",
        note="Default is to use `modes.walking.speed` as snap speed.",
    )
    input_files = {
        "distances": InputFile(
            TripsPedestrianDistancesFile,
            when=lambda inst: inst.distance_type == "pedestrian",
            when_doc='`modes.walking.distance.type` is `"pedestrian"`',
        ),
        "snap_distances": InputFile(
            TripsPedestrianNodesFile,
            when=lambda inst: inst.with_snap,
            when_doc="`modes.walking.distance.with_snap` is `true`",
        ),
    }
    output_files = {"tts": WalkingTravelTimesFile}

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
            walking_travel_time=pl.duration(
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
                walking_travel_time=pl.col("walking_travel_time") + pl.col("snap_travel_time"),
            )
        self.output["tts"].write(df)
