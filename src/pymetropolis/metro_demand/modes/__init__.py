from .bicycle import BicyclePreferencesFromPopulationStep, BicyclePreferencesStep
from .car import CAR_FILES, CAR_STEPS
from .files import (
    BicyclePreferencesFile,
    OutsideOptionPreferencesFile,
    OutsideOptionTravelTimesFile,
    PublicTransitPreferencesFile,
    PublicTransitTravelTimesFile,
    WalkingPreferencesFile,
)
from .outside_option import (
    OutsideOptionPreferencesStep,
    OutsideOptionTravelTimesFromRoadDistancesStep,
)
from .public_transit import (
    PublicTransitPreferencesFromPopulationStep,
    PublicTransitPreferencesStep,
    PublicTransitTravelTimesFromRoadDistancesStep,
)
from .walking import WalkingPreferencesFromPopulationStep, WalkingPreferencesStep

PT_FILES = [PublicTransitPreferencesFile, PublicTransitTravelTimesFile]
OUTSIDE_FILES = [OutsideOptionPreferencesFile, OutsideOptionTravelTimesFile]
WALKING_FILES = [WalkingPreferencesFile]
BICYCLE_FILES = [BicyclePreferencesFile]

MODES_FILES = CAR_FILES + PT_FILES + WALKING_FILES + OUTSIDE_FILES + BICYCLE_FILES

PT_STEPS = [
    PublicTransitPreferencesStep,
    PublicTransitPreferencesFromPopulationStep,
    PublicTransitTravelTimesFromRoadDistancesStep,
]
OUTSIDE_STEPS = [OutsideOptionPreferencesStep, OutsideOptionTravelTimesFromRoadDistancesStep]
WALKING_STEPS = [WalkingPreferencesStep, WalkingPreferencesFromPopulationStep]
BICYCLE_STEPS = [BicyclePreferencesStep, BicyclePreferencesFromPopulationStep]

MODES_STEPS = CAR_STEPS + PT_STEPS + WALKING_STEPS + OUTSIDE_STEPS + BICYCLE_STEPS
