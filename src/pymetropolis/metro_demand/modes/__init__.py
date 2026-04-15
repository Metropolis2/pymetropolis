from pymetropolis.metro_demand.modes.walking import WalkingPreferencesStep

from .car import CAR_FILES, CAR_STEPS
from .files import (
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

PT_FILES = [PublicTransitPreferencesFile, PublicTransitTravelTimesFile]
OUTSIDE_FILES = [OutsideOptionPreferencesFile, OutsideOptionTravelTimesFile]
WALKING_FILES = [WalkingPreferencesFile]

MODES_FILES = CAR_FILES + PT_FILES + WALKING_FILES + OUTSIDE_FILES

PT_STEPS = [PublicTransitPreferencesStep, PublicTransitTravelTimesFromRoadDistancesStep]
OUTSIDE_STEPS = [OutsideOptionPreferencesStep, OutsideOptionTravelTimesFromRoadDistancesStep]
WALKING_STEPS = [WalkingPreferencesStep]

MODES_STEPS = CAR_STEPS + PT_STEPS + WALKING_STEPS + OUTSIDE_STEPS
