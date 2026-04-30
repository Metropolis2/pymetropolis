from .car_driver import CarDriverPreferencesStep
from .car_driver_with_passengers import CarDriverWithPassengersPreferencesStep
from .car_passenger import CarPassengerPreferencesStep
from .car_ridesharing import CarRidesharingPreferencesStep
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
    CarDriverWithPassengersPreferencesStep,
    CarPassengerPreferencesStep,
    CarRidesharingPreferencesStep,
    CarFuelStep,
]
