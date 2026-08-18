from .common import PURPOSES as PURPOSES
from .distance import TripDistancesStep
from .draws import UniformDrawsStep
from .eqasim import EqasimImportStep
from .files import (
    ActivitiesLocationsFile,
    CarsFile,
    HouseholdsFile,
    HouseholdsHomesFile,
    HouseholdsZonesFile,
    JointToursFile,
    PersonsFile,
    ToursFile,
    TripsDestinationsFile,
    TripsDistancesFile,
    TripsFile,
    TripsOriginsFile,
    TripsZonesFile,
    UniformDrawsFile,
)
from .generic import (
    ActivitiesLocationsFromTripsLocationsStep,
    GenericPopulationStep,
    PopulationFromTripCoordinatesStep,
)
from .tours import CreateToursStep
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
    ActivitiesLocationsFile,
    ToursFile,
    JointToursFile,
]
POPULATION_STEPS = [
    GenericPopulationStep,
    PopulationFromTripCoordinatesStep,
    UniformDrawsStep,
    EqasimImportStep,
    TripDistancesStep,
    FrenchHouseholdsHomesZonesStep,
    FrenchTripsZonesStep,
    ActivitiesLocationsFromTripsLocationsStep,
    CreateToursStep,
]
