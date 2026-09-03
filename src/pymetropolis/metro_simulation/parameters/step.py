import json
from datetime import timedelta
from math import inf, isfinite

from pymetropolis.common import ThreadedStep
from pymetropolis.metro_calibration.road.files import TomTomCongestionTimesFile
from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_pipeline import Step
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
    MetroExAnteAgentsFile,
    MetroExAnteAlternativesFile,
    MetroExAnteTripsFile,
    MetroTripsFile,
)
from pymetropolis.metro_simulation.supply.files import MetroEdgesFile, MetroVehicleTypesFile

from .file import MetroExAnteParametersFile, MetroParametersFile


class StepWithPeriod(Step):
    """Abstract Step that holds the `simulation.period` parameter."""

    period = ListParameter(
        "simulation.period",
        inner=Time(),
        length=2,
        description="Time window to be simulated.",
        example="`[06:00:00, 10:00:00]`",
        note="The window can span multiple days.",
    )


class AbstractWriteMetroParametersStep(StepWithPeriod, ThreadedStep):
    """Abstract Step for the generation of Metropolis-Core parameters.

    Only the input files ("agents", "alternatives", "trips", "edges", "vehicle_types") and output
    file ("parameters") need to be defined.
    """

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
    node_order_reuse_threshold = DurationParameter(
        "simulation.node_order_reuse_threshold",
        default=timedelta(seconds=1),
        description=(
            "Threshold for expected edge TTFs RMSE above which node ordering is recomputed at next "
            "iteration."
        ),
        note=(
            "Edge TTFs RMSE is the RMSE of the difference between expected edge-level road travel "
            "times at the previous and current iteration."
        ),
    )

    def output_directory(self) -> str:
        raise NotImplementedError

    def is_defined(self) -> bool:
        return (
            self.period is not None
            and self.recording_interval is not None
            and (not self.spillback or self.max_pending_duration is not None)
        )

    def run(self):
        params = self.get_parameters()
        params_str = json.dumps(params, indent=2, sort_keys=True)
        self.output["parameters"].write(params_str)

    def get_parameters(self) -> dict:
        assert self.period is not None
        assert self.recording_interval is not None
        assert not self.spillback or self.max_pending_duration is not None
        assert self.departure_time_interval is not None
        assert self.backward_wave_speed is not None
        assert self.node_order_reuse_threshold is not None

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
            "output_directory": self.output_directory(),
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
            "node_order_reuse_threshold": self.node_order_reuse_threshold.total_seconds(),
        }
        if self.max_pending_duration is not None:
            params["road_network"]["max_pending_duration"] = (
                self.max_pending_duration.total_seconds()
            )
        backward_wave_speed = self.backward_wave_speed
        if isfinite(backward_wave_speed):
            params["road_network"]["backward_wave_speed"] = backward_wave_speed
        return params


class WriteMetroParametersStep(AbstractWriteMetroParametersStep):
    """Generates the input parameters file for the Metropolis-Core simulation."""

    input_files = {
        "agents": MetroAgentsFile,
        "alternatives": MetroAlternativesFile,
        "edges": InputFile(MetroEdgesFile, optional=True),
        "vehicle_types": InputFile(MetroVehicleTypesFile, optional=True),
        "trips": InputFile(MetroTripsFile, optional=True),
        "tmp": TomTomCongestionTimesFile,
    }
    output_files = {"parameters": MetroParametersFile}

    def output_directory(self) -> str:
        return "output"


class WriteExAnteMetroParametersStep(AbstractWriteMetroParametersStep):
    """Generates the input parameters file for the ex-ante simulation."""

    # Overwrite the nb_iterations parameter.
    nb_iterations = IntParameter(
        "simulation.ex_ante_nb_iterations",
        default=1,
        description="Number of iterations for the ex-ante simulation.",
    )
    priority = 0

    input_files = {
        "agents": MetroExAnteAgentsFile,
        "alternatives": MetroExAnteAlternativesFile,
        "edges": InputFile(MetroEdgesFile, optional=True),
        "vehicle_types": InputFile(MetroVehicleTypesFile, optional=True),
        "trips": InputFile(MetroExAnteTripsFile, optional=True),
    }
    output_files = {"parameters": MetroExAnteParametersFile}

    def output_directory(self) -> str:
        return "ex_ante_output"
