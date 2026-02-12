import subprocess

from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import PathParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_simulation.demand import (
    MetroAgentsFile,
    MetroAlternativesFile,
    MetroTripsFile,
)
from pymetropolis.metro_simulation.parameters import MetroParametersFile
from pymetropolis.metro_simulation.supply import MetroEdgesFile, MetroVehicleTypesFile

from .files import (
    MetroAgentResultsFile,
    MetroExpectedTravelTimeFunctionsFile,
    MetroIterationResultsFile,
    MetroNextExpectedTravelTimeFunctionsFile,
    MetroSimulatedTravelTimeFunctionsFile,
    MetroTripResultsFile,
)


class RunSimulationStep(Step):
    """Runs the Metropolis-Core simulation.

    This Step can take a few hours or even days to execute for large-scale simulations.
    """

    exec_path = PathParameter(
        "metropolis_core.exec_path",
        check_file_exists=True,
        description="Path to the `metropolis_cli` executable.",
        note='On Windows, you can ommit the ".exe" extension',
    )
    input_files = {
        "metro_parameters": MetroParametersFile,
        "metro_agents": MetroAgentsFile,
        "metro_alternatives": MetroAlternativesFile,
        "metro_edges": MetroEdgesFile,
        "metro_vehicle_types": MetroVehicleTypesFile,
        "metro_trips": InputFile(MetroTripsFile, optional=True),
    }
    output_files = {
        "metro_iteration_results": MetroIterationResultsFile,
        "metro_agent_results": MetroAgentResultsFile,
        "metro_trip_results": MetroTripResultsFile,
        "metro_sim_ttfs": MetroSimulatedTravelTimeFunctionsFile,
        "metro_exp_ttfs": MetroExpectedTravelTimeFunctionsFile,
        "metro_next_exp_ttfs": MetroNextExpectedTravelTimeFunctionsFile,
    }

    def run(self):
        # TODO. Check that metropolis_cli is a sufficiently recent version.
        self.check_exec_path()
        params_path = self.input["metro_parameters"].get_path()
        res = subprocess.run([self.exec_path, params_path])
        if res.returncode:
            # The run did not succeed.
            raise MetropyError("Metropolis-Core simulation failed.")
        for ofile in self.output.values():
            if not ofile.exists():
                raise MetropyError(f"Output file not written: `{ofile.get_path()}`")

    def check_exec_path(self):
        if not self.exec_path.is_file() and not self.exec_path.with_suffix(".exe").is_file():
            raise MetropyError(f"Cannot find the Metropolis-Core executable at `{self.exec_path}`")
