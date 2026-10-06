from .files import ParkAndRidePreferencesFile, ParkAndRideStopsFile
from .preferences import ParkAndRidePreferencesFromPopulationStep, ParkAndRidePreferencesStep
from .transfer_stops import ParkAndRideFacilitiesFromNearestStopStep

PARK_AND_RIDE_FILES = [ParkAndRidePreferencesFile, ParkAndRideStopsFile]

PARK_AND_RIDE_STEPS = [
    ParkAndRideFacilitiesFromNearestStopStep,
    ParkAndRidePreferencesStep,
    ParkAndRidePreferencesFromPopulationStep,
]
