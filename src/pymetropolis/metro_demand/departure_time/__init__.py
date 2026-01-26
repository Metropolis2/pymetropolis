from .linear_schedule_utility import (
    HomogeneousTstarStep,
    LinearScheduleStep,
)
from .linear_schedule_utility import (
    LinearScheduleFile as LinearScheduleFile,
)
from .linear_schedule_utility import (
    TstarsFile as TstarsFile,
)

DEPARTURE_TIME_FILES = [LinearScheduleFile, TstarsFile]
DEPARTURE_TIME_STEPS = [LinearScheduleStep, HomogeneousTstarStep]
