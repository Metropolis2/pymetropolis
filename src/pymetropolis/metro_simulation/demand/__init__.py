from .agents import PrepareMetroAgentsStep, WriteMetroAgentsStep
from .alternatives import PrepareMetroAlternativesStep, WriteMetroAlternativesStep
from .files import (
    MetroAgentsFile,
    MetroAgentsPopulationFile,
    MetroAlternativesFile,
    MetroAlternativesPopulationFile,
    MetroTripsFile,
    MetroTripsPopulationFile,
)
from .trips import PrepareMetroTripsStep, WriteMetroTripsStep

DEMAND_FILES = [
    MetroAgentsFile,
    MetroAlternativesFile,
    MetroTripsFile,
    MetroAgentsPopulationFile,
    MetroAlternativesPopulationFile,
    MetroTripsPopulationFile,
]
DEMAND_STEPS = [
    PrepareMetroAgentsStep,
    PrepareMetroAlternativesStep,
    PrepareMetroTripsStep,
    WriteMetroAgentsStep,
    WriteMetroAlternativesStep,
    WriteMetroTripsStep,
]
