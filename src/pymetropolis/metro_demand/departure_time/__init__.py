from pymetropolis.metro_pipeline import ConfigTable

from .linear_schedule_utility import (
    CONSTANT_TSTAR_STEP,
    LINEAR_SCHEDULE_STEP,
    LINEAR_SCHEDULE_TABLE,
)
from .linear_schedule_utility import LINEAR_SCHEDULE_FILE as LINEAR_SCHEDULE_FILE
from .linear_schedule_utility import TSTAR_FILE as TSTAR_FILE

DEPARTURE_TIME_TABLE = ConfigTable(
    "departure_time", "departure_time", items=[LINEAR_SCHEDULE_TABLE]
)

DEPARTURE_TIME_CONFIG_SCHEMA = [DEPARTURE_TIME_TABLE]
DEPARTURE_TIME_STEPS = [LINEAR_SCHEDULE_STEP, CONSTANT_TSTAR_STEP]
