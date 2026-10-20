from pymetropolis.modes import (
    CarDriver,
    CarDriverWithPassengers,
    CarPassenger,
    CarRidesharing,
    Truck,
)

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
    TruckPreferencesFile,
)
from .truck import TruckPreferencesFromPopulationStep, TruckPreferencesStep

CAR_PREFERENCES_FILES = {
    CarDriver: CarDriverPreferencesFile,
    CarDriverWithPassengers: CarDriverWithPassengersPreferencesFile,
    CarPassenger: CarPassengerPreferencesFile,
    CarRidesharing: CarRidesharingPreferencesFile,
    Truck: TruckPreferencesFile,
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
    TruckPreferencesStep,
    TruckPreferencesFromPopulationStep,
]
