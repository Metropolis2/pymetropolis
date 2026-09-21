from pymetropolis.modes import Bicycle, PublicTransit, Walking

from .bicycle import (
    BicyclePreferencesFromPopulationStep,
    BicyclePreferencesStep,
    BicycleTravelTimesFromDistanceStep,
)
from .car import CAR_FILES, CAR_PREFERENCES_FILES, CAR_STEPS
from .files import (
    BicyclePreferencesFile,
    BicycleTravelTimesFile,
    ModeChoiceMuFile,
    OutsideOptionPreferencesFile,
    OutsideOptionTravelTimesFile,
    PublicTransitPreferencesFile,
    WalkingPreferencesFile,
    WalkingTravelTimesFile,
)
from .mode_choice import ModeChoiceMuFromPopulationStep, ModeChoiceMuStep
from .outside_option import (
    OutsideOptionPreferencesStep,
    OutsideOptionTravelTimesFromRoadDistancesStep,
)
from .public_transit import (
    PublicTransitPreferencesFromPopulationStep,
    PublicTransitPreferencesStep,
    PublicTransitTravelTimesFromRoadDistancesStep,
)
from .walking import (
    WalkingPreferencesFromPopulationStep,
    WalkingPreferencesStep,
    WalkingTravelTimesFromDistanceStep,
)

PT_FILES = [PublicTransitPreferencesFile]
OUTSIDE_FILES = [OutsideOptionPreferencesFile, OutsideOptionTravelTimesFile]
WALKING_FILES = [WalkingPreferencesFile, WalkingTravelTimesFile]
BICYCLE_FILES = [BicyclePreferencesFile, BicycleTravelTimesFile]

MODES_FILES = (
    CAR_FILES + PT_FILES + WALKING_FILES + OUTSIDE_FILES + BICYCLE_FILES + [ModeChoiceMuFile]
)

# Preferences file of each trip-based mode, i.e., the modes whose constant and value of time are
# defined for each tour (the outside option is excluded: it has no trip and its own preferences
# file is read directly by `PrepareMetroAlternativesStep`).
MODE_PREFERENCES_FILES = {
    **CAR_PREFERENCES_FILES,
    PublicTransit: PublicTransitPreferencesFile,
    Walking: WalkingPreferencesFile,
    Bicycle: BicyclePreferencesFile,
}

PT_STEPS = [
    PublicTransitPreferencesStep,
    PublicTransitPreferencesFromPopulationStep,
    PublicTransitTravelTimesFromRoadDistancesStep,
]
OUTSIDE_STEPS = [OutsideOptionPreferencesStep, OutsideOptionTravelTimesFromRoadDistancesStep]
WALKING_STEPS = [
    WalkingPreferencesStep,
    WalkingPreferencesFromPopulationStep,
    WalkingTravelTimesFromDistanceStep,
]
BICYCLE_STEPS = [
    BicyclePreferencesStep,
    BicyclePreferencesFromPopulationStep,
    BicycleTravelTimesFromDistanceStep,
]
MU_STEPS = [ModeChoiceMuStep, ModeChoiceMuFromPopulationStep]

MODES_STEPS = CAR_STEPS + PT_STEPS + WALKING_STEPS + OUTSIDE_STEPS + BICYCLE_STEPS + MU_STEPS
