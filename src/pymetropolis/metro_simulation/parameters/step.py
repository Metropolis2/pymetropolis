import json
from math import inf, isfinite

from pymetropolis.common import ThreadedStep
from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_pipeline.parameters import (
    BoolParameter,
    DurationParameter,
    EnumParameter,
    FloatParameter,
    IntParameter,
    ListParameter,
)
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_pipeline.types import Time
from pymetropolis.metro_simulation.demand.files import (
    MetroAgentsFile,
    MetroAlternativesFile,
    MetroTripsFile,
)
from pymetropolis.metro_simulation.supply.files import MetroEdgesFile, MetroVehicleTypesFile

from .file import MetroParametersFile


class WriteMetroParametersStep(ThreadedStep):
    """Generates the input parameters file for the Metropolis-Core simulation."""

    period = ListParameter(
        "simulation.period",
        inner=Time(),
        length=2,
        description="Time window to be simulated.",
        example="`[06:00:00, 10:00:00]`",
        note="The window can span multiple days.",
    )
    departure_time_interval = DurationParameter(
        "simulation.departure_time_interval",
        description=(
            "Interval between two breakpoints in the utility function for departure-time choice."
        ),
        default=60.0,
        note=(
            "Smaller values make the simulation faster but can lead to approximations in the "
            "departure-time choice."
        ),
    )
    recording_interval = DurationParameter(
        "simulation.recording_interval",
        description="Time interval between two breakpoints for the travel-time functions.",
    )
    spillback = BoolParameter(
        "simulation.spillback",
        default=False,
        description=(
            "Whether the number of vehicles on a road should be limited by the total road length."
        ),
    )
    max_pending_duration = DurationParameter(
        "simulation.max_pending_duration",
        description=(
            "Maximum amount of time that a vehicle can spend waiting to enter the next road, "
            "in case of spillback."
        ),
    )
    backward_wave_speed = FloatParameter(
        "simulation.backward_wave_speed",
        default=inf,
        description=(
            "Speed at which the holes created by a vehicle leaving a road is propagating backward "
            "(in km/h)."
        ),
    )
    learning_factor = FloatParameter(
        "simulation.learning_factor",
        default=0.0,
        description="Value of the smoothing factor for the exponential learning model.",
        note=(
            "Value must be between 0 and 1. Smaller values lead to slower but steadier "
            "convergences."
        ),
    )
    routing_algorithm = EnumParameter(
        "simulation.routing_algorithm",
        values=["Best", "Intersect", "TCH"],
        default="Best",
        description=(
            "Algorithm type to use when computing the origin-destination travel-time functions."
        ),
        note='Possible values: "Best", "Intersect", "TCH"',
    )
    nb_iterations = IntParameter(
        "simulation.nb_iterations", default=1, description="Number of iterations to be simulated."
    )
    input_files = {
        "agents": MetroAgentsFile,
        "alternatives": MetroAlternativesFile,
        "edges": InputFile(MetroEdgesFile, optional=True),
        "vehicle_types": InputFile(MetroVehicleTypesFile, optional=True),
        "trips": InputFile(MetroTripsFile, optional=True),
    }
    output_files = {"parameters": MetroParametersFile}

    def is_defined(self) -> bool:
        return (
            self.period is not None
            and self.recording_interval is not None
            and (not self.spillback or self.max_pending_duration is not None)
        )

    def run(self):
        t0, t1 = self.period
        if t1 <= t0:
            raise MetropyError(
                "Invalid simulation period: end time must be larger than start time."
            )
        period = [t0.seconds(), t1.seconds()]
        recording_interval = self.recording_interval.total_seconds()
        # `wdir` is the working directory from which Metropolis-Core is run.
        # Input file paths can be defined relative to the working directory.
        wdir = self.output["parameters"].complete_path.parent
        params = {
            "input_files": {
                "agents": self.input["agents"].relative_path_from(wdir),
                "alternatives": self.input["alternatives"].relative_path_from(wdir),
            },
            "output_directory": "output",
            "period": period,
            "departure_time_interval": self.departure_time_interval.total_seconds(),
            "learning_model": {"type": "Exponential", "value": self.learning_factor},
            "max_iterations": self.nb_iterations,
            "saving_format": "Parquet",
            "nb_threads": self.nb_threads or 0,
        }
        for name in ("edges", "vehicle_types", "trips"):
            if self.input[name].exists():
                params["input_files"][name] = self.input[name].relative_path_from(wdir)
        params["road_network"] = {
            "recording_interval": recording_interval,
            "spillback": self.spillback,
            "algorithm_type": self.routing_algorithm,
        }
        if self.max_pending_duration is not None:
            params["road_network"]["max_pending_duration"] = (
                self.max_pending_duration.total_seconds()
            )
        backward_wave_speed = self.backward_wave_speed
        if isfinite(backward_wave_speed):
            params["road_network"]["backward_wave_speed"] = backward_wave_speed
        params_str = json.dumps(params, indent=2, sort_keys=True)
        self.output["parameters"].write(params_str)
