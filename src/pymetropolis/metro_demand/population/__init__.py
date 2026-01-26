from .draws import UniformDrawsStep
from .files import HouseholdsFile, PersonsFile, TripsFile, UniformDrawsFile
from .generic import GenericPopulationStep

POPULATION_FILES = [HouseholdsFile, PersonsFile, TripsFile, UniformDrawsFile]
POPULATION_STEPS = [GenericPopulationStep, UniformDrawsStep]
