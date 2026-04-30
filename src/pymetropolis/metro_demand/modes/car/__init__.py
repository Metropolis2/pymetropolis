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
    CarFuelFile,
    CarPassengerPreferencesFile,
    CarRidesharingPreferencesFile,
)
from .fuel_consumption import CarFuelStep

CAR_FILES = [
    CarDriverPreferencesFile,
    CarDriverWithPassengersPreferencesFile,
    CarPassengerPreferencesFile,
    CarRidesharingPreferencesFile,
    CarFuelFile,
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
    CarFuelStep,
]
