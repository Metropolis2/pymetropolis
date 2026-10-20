from .ex_ante import HomogeneousExAnteDepartureTimeStep
from .linear_schedule_utility import (
    HomogeneousTstarStep,
    LinearScheduleFromPurposeStep,
    LinearScheduleStep,
    TstarFromArrivalTimeStep,
)
from .linear_schedule_utility import LinearScheduleFile as LinearScheduleFile
from .linear_schedule_utility import TstarsFile as TstarsFile

DEPARTURE_TIME_FILES = [LinearScheduleFile, TstarsFile]
DEPARTURE_TIME_STEPS = [
    HomogeneousExAnteDepartureTimeStep,
    LinearScheduleStep,
    LinearScheduleFromPurposeStep,
    HomogeneousTstarStep,
    TstarFromArrivalTimeStep,
]
