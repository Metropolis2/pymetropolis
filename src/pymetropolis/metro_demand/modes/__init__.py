from .bicycle import BicyclePreferencesStep
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
    PublicTransitPreferencesStep,
    PublicTransitTravelTimesFromRoadDistancesStep,
)
from .walking import WalkingPreferencesStep

PT_FILES = [PublicTransitPreferencesFile, PublicTransitTravelTimesFile]
OUTSIDE_FILES = [OutsideOptionPreferencesFile, OutsideOptionTravelTimesFile]
WALKING_FILES = [WalkingPreferencesFile]
BICYCLE_FILES = [BicyclePreferencesFile]

MODES_FILES = CAR_FILES + PT_FILES + WALKING_FILES + OUTSIDE_FILES + BICYCLE_FILES

PT_STEPS = [PublicTransitPreferencesStep, PublicTransitTravelTimesFromRoadDistancesStep]
OUTSIDE_STEPS = [OutsideOptionPreferencesStep, OutsideOptionTravelTimesFromRoadDistancesStep]
WALKING_STEPS = [WalkingPreferencesStep]
BICYCLE_STEPS = [BicyclePreferencesStep]

MODES_STEPS = CAR_STEPS + PT_STEPS + WALKING_STEPS + OUTSIDE_STEPS + BICYCLE_STEPS
