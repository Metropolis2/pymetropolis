from pymetropolis.metro_pipeline import MetroFile, Step

from .crs import GeoStep as GeoStep
from .ign import IGN_STEPS
from .osm import OSMStep as OSMStep
from .simulation_area import SIMULATION_AREA_FILES, SIMULATION_AREA_STEPS
from .urban_areas import URBAN_AREAS_FILES, URBAN_AREAS_STEPS

FILES: list[type[MetroFile]] = SIMULATION_AREA_FILES + URBAN_AREAS_FILES
STEPS: list[type[Step]] = SIMULATION_AREA_STEPS + URBAN_AREAS_STEPS + IGN_STEPS
