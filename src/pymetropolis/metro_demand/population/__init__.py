from .distance import TripDistancesStep
from .draws import UniformDrawsStep
from .eqasim import EqasimImportStep
from .files import (
    CarsFile,
    HouseholdsFile,
    HouseholdsHomesFile,
    HouseholdsZonesFile,
    PersonsFile,
    TripsDestinationsFile,
    TripsDistancesFile,
    TripsFile,
    TripsOriginsFile,
    TripsZonesFile,
    UniformDrawsFile,
)
from .generic import GenericPopulationStep
from .zones import FrenchHouseholdsHomesZonesStep, FrenchTripsZonesStep

POPULATION_FILES = [
    HouseholdsFile,
    HouseholdsHomesFile,
    HouseholdsZonesFile,
    CarsFile,
    PersonsFile,
    TripsFile,
    TripsOriginsFile,
    TripsDestinationsFile,
    TripsDistancesFile,
    TripsZonesFile,
    UniformDrawsFile,
]
POPULATION_STEPS = [
    GenericPopulationStep,
    UniformDrawsStep,
    EqasimImportStep,
    TripDistancesStep,
    FrenchHouseholdsHomesZonesStep,
    FrenchTripsZonesStep,
]
