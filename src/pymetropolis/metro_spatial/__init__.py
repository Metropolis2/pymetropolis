from pymetropolis.metro_pipeline import MetroFile, Step

from .crs import GeoStep as GeoStep
from .osm import OSMStep as OSMStep
from .simulation_area import SIMULATION_AREA_FILES, SIMULATION_AREA_STEPS

FILES: list[type[MetroFile]] = SIMULATION_AREA_FILES
STEPS: list[type[Step]] = SIMULATION_AREA_STEPS
