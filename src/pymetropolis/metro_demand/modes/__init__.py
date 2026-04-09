from .car import CAR_FILES, CAR_STEPS
from .files import (
    OutsideOptionPreferencesFile,
    OutsideOptionTravelTimesFile,
    PublicTransitPreferencesFile,
    PublicTransitTravelTimesFile,
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

MODES_FILES = CAR_FILES + PT_FILES + OUTSIDE_FILES

PT_STEPS = [PublicTransitPreferencesStep, PublicTransitTravelTimesFromRoadDistancesStep]
OUTSIDE_STEPS = [OutsideOptionPreferencesStep, OutsideOptionTravelTimesFromRoadDistancesStep]

MODES_STEPS = CAR_STEPS + PT_STEPS + OUTSIDE_STEPS
