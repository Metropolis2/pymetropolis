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

MODES_FILES = CAR_FILES + [
    PublicTransitPreferencesFile,
    PublicTransitTravelTimesFile,
    OutsideOptionPreferencesFile,
    OutsideOptionTravelTimesFile,
]

MODES_STEPS = CAR_STEPS + [
    PublicTransitPreferencesStep,
    PublicTransitTravelTimesFromRoadDistancesStep,
    OutsideOptionPreferencesStep,
    OutsideOptionTravelTimesFromRoadDistancesStep,
]
