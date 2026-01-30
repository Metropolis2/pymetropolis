import json
from math import inf, isfinite

from pymetropolis.metro_common.utils import time_to_seconds_since_midnight
from pymetropolis.metro_pipeline.parameters import (
    BoolParameter,
    DurationParameter,
    EnumParameter,
    FloatParameter,
    IntParameter,
    ListParameter,
)
from pymetropolis.metro_pipeline.steps import Step
from pymetropolis.metro_pipeline.types import Time

from .file import MetroParametersFile


class WriteMetroParametersStep(Step):
    """Generates the input parameters file for the Metropolis-Core simulation."""

    period = ListParameter(
        "simulation_parameters.period",
        inner=Time(),
        length=2,
        description="Time window to be simulated.",
        example="`[00:00:00, 24:00:00]`",
    )
    recording_interval = DurationParameter(
        "simulation_parameters.recording_interval",
        description="Time interval between two breakpoints for the travel-time functions.",
    )
    spillback = BoolParameter(
        "simulation_parameters.spillback",
        default=False,
        description="Whether the number of vehicles on a road should be limited by the total road length.",
    )
    max_pending_duration = DurationParameter(
        "simulation_parameters.max_pending_duration",
        description="Maximum amount of time that a vehicle can spend waiting to enter the next road, in case of spillback.",
    )
    backward_wave_speed = FloatParameter(
        "simulation_parameters.backward_wave_speed",
        default=inf,
        description="Speed at which the holes created by a vehicle leaving a road is propagating backward (in km/h).",
    )
    learning_factor = FloatParameter(
        "simulation_parameters.learning_factor",
        default=0.0,
        description="Value of the smoothing factor for the exponential learning model.",
        note="Value must be between 0 and 1. Smaller values lead to slower but steadier convergences.",
    )
    routing_algorithm = EnumParameter(
        "simulation_parameters.routing_algorithm",
        values=["Best", "Intersect", "TCH"],
        default="Best",
        description="Algorithm type to use when computing the origin-destination travel-time functions.",
        note='Possible values: "Best", "Intersect", "TCH"',
    )
    nb_iterations = IntParameter(
        "simulation_parameters.nb_iterations",
        default=1,
        description="Number of iterations to be simulated.",
    )
    output_files = {"metro_parameters": MetroParametersFile}

    def is_defined(self) -> bool:
        return (
            self.period is not None
            and self.recording_interval is not None
            and (not self.spillback or self.max_pending_duration is not None)
        )

    def run(self):
        t0, t1 = self.period
        period = [time_to_seconds_since_midnight(t0), time_to_seconds_since_midnight(t1)]
        recording_interval = self.recording_interval.total_seconds()
        params = {
            "input_files": {
                # TODO: Is there any way not to hardcode these values?
                # TODO: What if there is no trips defined?
                "agents": "input/agents.parquet",
                "alternatives": "input/alts.parquet",
                "trips": "input/trips.parquet",
                "edges": "input/edges.parquet",
                "vehicle_types": "input/vehicle_types.parquet",
            },
            "output_directory": "output",
            "period": period,
            "learning_model": {
                "type": "Exponential",
                "value": self.learning_factor,
            },
            "max_iterations": self.nb_iterations,
            "saving_format": "Parquet",
        }
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
        self.output["metro_parameters"].write(params_str)
