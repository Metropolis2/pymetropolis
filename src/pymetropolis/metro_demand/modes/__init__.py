from .car_driver import CarDriverDistancesStep, CarDriverPreferencesStep
from .files import (
    CarDriverDistancesFile,
    CarDriverODsFile,
    CarDriverPreferencesFile,
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

MODES_FILES = [
    CarDriverPreferencesFile,
    CarDriverDistancesFile,
    CarDriverODsFile,
    PublicTransitPreferencesFile,
    PublicTransitTravelTimesFile,
    OutsideOptionPreferencesFile,
    OutsideOptionTravelTimesFile,
]

MODES_STEPS = [
    CarDriverPreferencesStep,
    CarDriverDistancesStep,
    PublicTransitPreferencesStep,
    PublicTransitTravelTimesFromRoadDistancesStep,
    OutsideOptionPreferencesStep,
    OutsideOptionTravelTimesFromRoadDistancesStep,
]
