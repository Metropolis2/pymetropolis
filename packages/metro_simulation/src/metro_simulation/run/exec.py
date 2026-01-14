import os
import subprocess

from metro_common.errors import MetropyError, error_context
from metro_common.types import Path
from metro_pipeline import Config, ConfigTable, ConfigValue, Step

from metro_simulation.demand import METRO_AGENTS_FILE, METRO_ALTS_FILE, METRO_TRIPS_FILE
from metro_simulation.parameters import PARAMETERS_FILE
from metro_simulation.supply import METRO_EDGES_FILE, METRO_VEHICLE_TYPES_FILE

from .files import METRO_AGENT_RESULTS_FILE, METRO_TRIP_RESULTS_FILE

EXEC_PATH = ConfigValue(
    "metropolis_core.exec_path",
    key="exec_path",
    expected_type=Path(),
    description="Path to the metropolis_cli executable.",
)

METROPOLIS_CORE_TABLE = ConfigTable(
    "metropolis_core",
    "metropolis_core",
    items=[EXEC_PATH],
)


@error_context(msg="Cannot execute Metropolis-Core")
def execute_metropolis_cli(config: Config):
    exec_path = config[EXEC_PATH]
    if not os.path.isfile(exec_path):
        raise MetropyError(f"Invalid path to metropolis_cli: `{exec_path}`")
    params_path = PARAMETERS_FILE.get_path(config)
    res = subprocess.run([exec_path, params_path])
    if res.returncode:
        # The run did not succeed.
        return False
    if not os.path.isfile(METRO_TRIP_RESULTS_FILE.get_path(config)):
        raise MetropyError(f"Output file not written: `{exec_path}`")
    if not os.path.isfile(METRO_AGENT_RESULTS_FILE.get_path(config)):
        raise MetropyError(f"Output file not written: `{exec_path}`")
    return True


RUN_SIMULATION_STEP = Step(
    "run-simulation",
    execute_metropolis_cli,
    required_files=[
        PARAMETERS_FILE,
        METRO_AGENTS_FILE,
        METRO_ALTS_FILE,
        METRO_TRIPS_FILE,
        METRO_EDGES_FILE,
        METRO_VEHICLE_TYPES_FILE,
    ],
    output_files=[METRO_TRIP_RESULTS_FILE, METRO_AGENT_RESULTS_FILE],
    config_values=[EXEC_PATH],
)
