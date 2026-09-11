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
    "car_driver": CarDriverPreferencesFile,
    "car_driver_with_passengers": CarDriverWithPassengersPreferencesFile,
    "car_passenger": CarPassengerPreferencesFile,
    "car_ridesharing": CarRidesharingPreferencesFile,
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
