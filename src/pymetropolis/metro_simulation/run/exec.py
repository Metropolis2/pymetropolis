import subprocess

from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import ExecPathParameter
from pymetropolis.metro_simulation.parameters import MetroParametersFile
from pymetropolis.metro_simulation.parameters.file import MetroExAnteParametersFile

from .files import (
    MetroAgentResultsFile,
    MetroExAnteAgentResultsFile,
    MetroExAnteExpectedTravelTimeFunctionsFile,
    MetroExAnteIterationResultsFile,
    MetroExAnteNextExpectedTravelTimeFunctionsFile,
    MetroExAnteRouteResultsFile,
    MetroExAnteSimulatedTravelTimeFunctionsFile,
    MetroExAnteTripResultsFile,
    MetroExpectedTravelTimeFunctionsFile,
    MetroIterationResultsFile,
    MetroNextExpectedTravelTimeFunctionsFile,
    MetroRouteResultsFile,
    MetroSimulatedTravelTimeFunctionsFile,
    MetroTripResultsFile,
)


class AbstractRunSimulationStep(Step):
    """Abstract Step for simulation runs."""

    exec_path = ExecPathParameter(
        "metropolis_core.exec_path",
        description="Path to the `metropolis_cli` executable.",
        default="env:METROPOLIS_EXEC_PATH",
        note='On Windows, you can omit the ".exe" extension',
    )

    def is_defined(self):
        return self.exec_path is not None

    def run(self):
        assert self.exec_path is not None
        # TODO. Check that metropolis_cli is a sufficiently recent version.
        params_path = self.input["parameters"].get_path()
        res = subprocess.run([self.exec_path, params_path], check=False)
        if res.returncode:
            # The run did not succeed.
            raise MetropyError("Metropolis-Core simulation failed.")
        for ofile in self.output.values():
            if not ofile.exists():
                raise MetropyError(f"Output file not written: `{ofile.get_path()}`")


class RunSimulationStep(AbstractRunSimulationStep):
    """Runs the Metropolis-Core simulation.

    This Step can take a few hours or even days to execute for large-scale simulations.
    """

    # The Step depends only on the parameters.json, which itself depends on all the input files.
    input_files = {"parameters": MetroParametersFile}
    output_files = {
        "metro_iteration_results": MetroIterationResultsFile,
        "metro_agent_results": MetroAgentResultsFile,
        "metro_trip_results": MetroTripResultsFile,
        "metro_route_results": MetroRouteResultsFile,
        "metro_sim_ttfs": MetroSimulatedTravelTimeFunctionsFile,
        "metro_exp_ttfs": MetroExpectedTravelTimeFunctionsFile,
        "metro_next_exp_ttfs": MetroNextExpectedTravelTimeFunctionsFile,
    }


class RunExAnteSimulationStep(AbstractRunSimulationStep):
    """Runs the ex-ante simulation.

    The ex-ante simulation is a complete simulation with modes and departure times fixed to their
    ex-ante values.
    """

    # The Step depends only on the parameters.json, which itself depends on all the input files.
    input_files = {"parameters": MetroExAnteParametersFile}
    output_files = {
        "metro_iteration_results": MetroExAnteIterationResultsFile,
        "metro_agent_results": MetroExAnteAgentResultsFile,
        "metro_trip_results": MetroExAnteTripResultsFile,
        "metro_route_results": MetroExAnteRouteResultsFile,
        "metro_sim_ttfs": MetroExAnteSimulatedTravelTimeFunctionsFile,
        "metro_exp_ttfs": MetroExAnteExpectedTravelTimeFunctionsFile,
        "metro_next_exp_ttfs": MetroExAnteNextExpectedTravelTimeFunctionsFile,
    }
