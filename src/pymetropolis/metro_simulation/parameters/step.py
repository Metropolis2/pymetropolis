import json
from datetime import timedelta
from math import inf, isfinite

from pymetropolis.metro_common.errors import error_context
from pymetropolis.metro_common.types import Duration, FixedSizeList, Time
from pymetropolis.metro_common.utils import time_to_seconds_since_midnight
from pymetropolis.metro_pipeline import Config, ConfigTable, ConfigValue, Step

from .file import PARAMETERS_FILE

PERIOD = ConfigValue(
    "simulation_parameters.period",
    key="period",
    expected_type=FixedSizeList(2, Time()),
    description="Time window to be simulated.",
)

RECORDING_INTERVAL = ConfigValue(
    "simulation_parameters.recording_interval",
    key="recording_interval",
    expected_type=Duration(),
    description="Time interval between two breakpoints for the travel-time functions.",
)

SPILLBACK = ConfigValue(
    "simulation_parameters.spillback",
    key="spillback",
    expected_type=bool,
    default=False,
    description="Whether the number of vehicles on a road should be limited by the total road length.",
)

MAX_PENDING_DURATION = ConfigValue(
    "simulation_parameters.max_pending_duration",
    key="max_pending_duration",
    expected_type=Duration(),
    default=timedelta(seconds=300),
    description="Maximum amount of time that a vehicle can spend waiting to enter the next road, in case of spillback.",
)

BACKWARD_WAVE_SPEED = ConfigValue(
    "simulation_parameters.backward_wave_speed",
    key="backward_wave_speed",
    expected_type=float,
    default=inf,
    description="Speed at which the holes created by a vehicle leaving a road is propagating backward (in km/h).",
)

LEARNING_FACTOR = ConfigValue(
    "simulation_parameters.learning_factor",
    key="learning_factor",
    expected_type=float,
    default=0.0,
    description="Value of the smoothing factor for the exponential learning model.",
    note="Value must be between 0 and 1. Smaller values lead to slower but steadier convergences.",
)

ROUTING_ALGORITHM = ConfigValue(
    "simulation_parameters.routing_algorithm",
    key="routing_algorithm",
    expected_type=str,
    default="Best",
    description="Algorithm type to use when computing the origin-destination travel-time functions.",
    note='Possible values: "Best", "Intersect", "TCH"',
)

NB_ITERATIONS = ConfigValue(
    "simulation_parameters.nb_iterations",
    key="nb_iterations",
    expected_type=int,
    default=1,
    description="Number of iterations to be simulated.",
)

PARAMETERS_TABLE = ConfigTable(
    "simulation_parameters",
    "simulation_parameters",
    items=[
        PERIOD,
        RECORDING_INTERVAL,
        SPILLBACK,
        MAX_PENDING_DURATION,
        BACKWARD_WAVE_SPEED,
        LEARNING_FACTOR,
        ROUTING_ALGORITHM,
        NB_ITERATIONS,
    ],
)


@error_context(msg="Cannot write parameters file")
def write_parameters(config: Config):
    t0, t1 = config[PERIOD]
    period = [time_to_seconds_since_midnight(t0), time_to_seconds_since_midnight(t1)]
    recording_interval = config[RECORDING_INTERVAL].total_seconds()
    max_pending_duration = config[MAX_PENDING_DURATION].total_seconds()
    params = {
        "input_files": {
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
            "value": config[LEARNING_FACTOR],
        },
        "road_network": {
            "recording_interval": recording_interval,
            "spillback": config[SPILLBACK],
            "max_pending_duration": max_pending_duration,
            "algorithm_type": config[ROUTING_ALGORITHM],
        },
        "max_iterations": config[NB_ITERATIONS],
        "saving_format": "Parquet",
    }
    backward_wave_speed = config[BACKWARD_WAVE_SPEED]
    if isfinite(backward_wave_speed):
        params["road_network"]["backward_wave_speed"] = backward_wave_speed
    params_str = json.dumps(params, indent=2, sort_keys=True)
    PARAMETERS_FILE.save(params_str, config)
    return True


WRITE_PARAMETERS_STEP = Step(
    "write-parameters",
    write_parameters,
    output_files=[PARAMETERS_FILE],
    config_values=[
        PERIOD,
        RECORDING_INTERVAL,
        SPILLBACK,
        MAX_PENDING_DURATION,
        BACKWARD_WAVE_SPEED,
        LEARNING_FACTOR,
        ROUTING_ALGORITHM,
        NB_ITERATIONS,
    ],
)
