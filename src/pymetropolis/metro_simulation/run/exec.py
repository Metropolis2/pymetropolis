import subprocess

from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import PathParameter
from pymetropolis.metro_simulation.demand import (
    MetroAgentsFile,
    MetroAlternativesFile,
    MetroTripsFile,
)
from pymetropolis.metro_simulation.parameters import MetroParametersFile
from pymetropolis.metro_simulation.supply import MetroEdgesFile, MetroVehicleTypesFile

from .files import MetroAgentResultsFile, MetroTripResultsFile


class RunSimulationStep(Step):
    """Runs the Metropolis-Core simulation.

    This Step can take a few hours or even days to execute for large-scale simulations.
    """

    exec_path = PathParameter(
        "metropolis_core.exec_path",
        check_file_exists=True,
        description="Path to the metropolis_cli executable.",
    )
    output_files = {
        "metro_agent_results": MetroAgentResultsFile,
        "metro_trip_results": MetroTripResultsFile,
    }

    def required_files(self):
        return {
            "metro_parameters": MetroParametersFile,
            "metro_agents": MetroAgentsFile,
            "metro_alternatives": MetroAlternativesFile,
            "metro_edges": MetroEdgesFile,
            "metro_vehicle_types": MetroVehicleTypesFile,
        }

    def optional_files(self):
        # Simulations can actually be run without any trips.
        return {"metro_trips": MetroTripsFile}

    def run(self):
        params_path = self.input["metro_parameters"].get_path()
        res = subprocess.run([self.exec_path, params_path])
        if res.returncode:
            # The run did not succeed.
            raise MetropyError("Metropolis-Core simulation failed.")
        path = self.output["metro_trip_results"]
        if not path.exists():
            raise MetropyError(f"Output file not written: `{path.get_path()}`")
        path = self.output["metro_agent_results"]
        if not path.exists():
            raise MetropyError(f"Output file not written: `{path.get_path()}`")
