from pymetropolis.modes import CarDriver, CarDriverWithPassengers, CarPassenger, CarRidesharing

from .car_driver import CarDriverPreferencesFromPopulationStep, CarDriverPreferencesStep
from .car_driver_with_passengers import (
    CarDriverWithPassengersPreferencesFromPopulationStep,
    CarDriverWithPassengersPreferencesStep,
)
from .car_passenger import CarPassengerPreferencesFromPopulationStep, CarPassengerPreferencesStep
from .car_ridesharing import (
    CarRidesharingPreferencesFromPopulationStep,
    CarRidesharingPreferencesStep,
)
from .files import (
    CarDriverPreferencesFile,
    CarDriverWithPassengersPreferencesFile,
    CarPassengerPreferencesFile,
    CarRidesharingPreferencesFile,
)

CAR_PREFERENCES_FILES = {
    CarDriver: CarDriverPreferencesFile,
    CarDriverWithPassengers: CarDriverWithPassengersPreferencesFile,
    CarPassenger: CarPassengerPreferencesFile,
    CarRidesharing: CarRidesharingPreferencesFile,
}

CAR_FILES = list(CAR_PREFERENCES_FILES.values())

CAR_STEPS = [
    CarDriverPreferencesStep,
    CarDriverPreferencesFromPopulationStep,
    CarDriverWithPassengersPreferencesStep,
    CarDriverWithPassengersPreferencesFromPopulationStep,
    CarPassengerPreferencesStep,
    CarPassengerPreferencesFromPopulationStep,
    CarRidesharingPreferencesStep,
    CarRidesharingPreferencesFromPopulationStep,
]
