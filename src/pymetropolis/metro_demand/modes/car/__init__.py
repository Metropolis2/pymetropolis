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

CAR_FILES = [
    CarDriverPreferencesFile,
    CarDriverWithPassengersPreferencesFile,
    CarPassengerPreferencesFile,
    CarRidesharingPreferencesFile,
]

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
