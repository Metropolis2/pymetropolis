from .car_driver import CarDriverPreferencesStep
from .car_driver_with_passengers import CarDriverWithPassengersPreferencesStep
from .car_passenger import CarPassengerPreferencesStep
from .car_ridesharing import CarRidesharingPreferencesStep
from .distances import CarFreeFlowDistancesStep, CarShortestDistancesStep
from .files import (
    CarDriverPreferencesFile,
    CarDriverWithPassengersPreferencesFile,
    CarFreeFlowDistancesFile,
    CarFuelFile,
    CarODsFile,
    CarPassengerPreferencesFile,
    CarRidesharingPreferencesFile,
    CarShortestDistancesFile,
)
from .fuel_consumption import CarFuelStep

CAR_FILES = [
    CarDriverPreferencesFile,
    CarDriverWithPassengersPreferencesFile,
    CarPassengerPreferencesFile,
    CarRidesharingPreferencesFile,
    CarODsFile,
    CarShortestDistancesFile,
    CarFreeFlowDistancesFile,
    CarFuelFile,
]

CAR_STEPS = [
    CarDriverPreferencesStep,
    CarDriverWithPassengersPreferencesStep,
    CarPassengerPreferencesStep,
    CarRidesharingPreferencesStep,
    CarShortestDistancesStep,
    CarFreeFlowDistancesStep,
    CarFuelStep,
]
