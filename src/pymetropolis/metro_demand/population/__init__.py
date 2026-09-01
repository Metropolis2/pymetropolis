from .common import PURPOSES as PURPOSES
from .distance import TripDistancesStep
from .draws import UniformDrawsStep
from .eqasim import EqasimImportStep
from .files import (
    ActivitiesLocationsFile,
    CarsFile,
    HouseholdsFile,
    HouseholdsHomesFile,
    HouseholdsHomesUrbanTypeFile,
    HouseholdsZonesFile,
    JointToursFile,
    PersonsFile,
    ToursFile,
    ToursModeFile,
    TripsDestinationsFile,
    TripsDistancesFile,
    TripsFile,
    TripsOriginsFile,
    TripsUrbanTypeFile,
    TripsZonesFile,
    UniformDrawsFile,
)
from .generic import (
    ActivitiesLocationsFromTripsLocationsStep,
    ImportTripCoordinatesStep,
    PopulationFromTripCoordinatesStep,
)
from .tours import CreateToursStep
from .urban_types import FrenchHouseholdsUrbanTypeStep, FrenchTripsUrbanTypeStep
from .zones import HouseholdsHomesZonesStep, TripsZonesStep

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
    ToursModeFile,
    HouseholdsHomesUrbanTypeFile,
    TripsUrbanTypeFile,
]
POPULATION_STEPS = [
    ImportTripCoordinatesStep,
    PopulationFromTripCoordinatesStep,
    UniformDrawsStep,
    EqasimImportStep,
    TripDistancesStep,
    HouseholdsHomesZonesStep,
    TripsZonesStep,
    ActivitiesLocationsFromTripsLocationsStep,
    CreateToursStep,
    FrenchHouseholdsUrbanTypeStep,
    FrenchTripsUrbanTypeStep,
]
