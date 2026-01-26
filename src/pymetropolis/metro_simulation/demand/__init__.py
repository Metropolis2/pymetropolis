from .agents import WriteMetroAgentsStep
from .alternatives import WriteMetroAlternativesStep
from .files import MetroAgentsFile, MetroAlternativesFile, MetroTripsFile
from .trips import WriteMetroTripsStep

DEMAND_FILES = [MetroTripsFile, MetroAgentsFile, MetroAlternativesFile]
DEMAND_STEPS = [WriteMetroAgentsStep, WriteMetroAlternativesStep, WriteMetroTripsStep]
