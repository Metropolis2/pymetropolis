from .agents import (
    PrepareExAnteMetroAgentsStep,
    PrepareMetroAgentsStep,
    WriteExAnteMetroAgentsStep,
    WriteMetroAgentsStep,
)
from .alternatives import (
    PrepareExAnteMetroAlternativesStep,
    PrepareMetroAlternativesStep,
    WriteExAnteMetroAlternativesStep,
    WriteMetroAlternativesStep,
)
from .files import (
    MetroAgentsFile,
    MetroAgentsPopulationFile,
    MetroAlternativesFile,
    MetroAlternativesPopulationFile,
    MetroExAnteAgentsFile,
    MetroExAnteAgentsPopulationFile,
    MetroExAnteAlternativesFile,
    MetroExAnteAlternativesPopulationFile,
    MetroExAnteTripsFile,
    MetroExAnteTripsPopulationFile,
    MetroTripsFile,
    MetroTripsPopulationFile,
)
from .trips import (
    PrepareExAnteMetroTripsStep,
    PrepareMetroTripsStep,
    WriteExAnteMetroTripsStep,
    WriteMetroTripsStep,
)

DEMAND_FILES = [
    MetroAgentsFile,
    MetroAlternativesFile,
    MetroTripsFile,
    MetroAgentsPopulationFile,
    MetroAlternativesPopulationFile,
    MetroTripsPopulationFile,
    MetroExAnteAgentsFile,
    MetroExAnteAgentsPopulationFile,
    MetroExAnteAlternativesFile,
    MetroExAnteAlternativesPopulationFile,
    MetroExAnteTripsFile,
    MetroExAnteTripsPopulationFile,
]
DEMAND_STEPS = [
    PrepareMetroAgentsStep,
    PrepareMetroAlternativesStep,
    PrepareMetroTripsStep,
    WriteMetroAgentsStep,
    WriteMetroAlternativesStep,
    WriteMetroTripsStep,
    PrepareExAnteMetroAgentsStep,
    PrepareExAnteMetroAlternativesStep,
    PrepareExAnteMetroTripsStep,
    WriteExAnteMetroAgentsStep,
    WriteExAnteMetroAlternativesStep,
    WriteExAnteMetroTripsStep,
]
