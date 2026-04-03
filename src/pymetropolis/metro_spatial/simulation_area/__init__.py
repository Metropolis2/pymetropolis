from pymetropolis.metro_pipeline import MetroFile, Step

from .aav import SimulationAreaFromAAVStep
from .bbox import SimulationAreaFromBboxStep
from .file import SimulationAreaFile
from .osm import SimulationAreaFromOSMStep
from .polygons import SimulationAreaFromPolygonsStep

SIMULATION_AREA_FILES: list[type[MetroFile]] = [SimulationAreaFile]
SIMULATION_AREA_STEPS: list[type[Step]] = [
    SimulationAreaFromBboxStep,
    SimulationAreaFromPolygonsStep,
    SimulationAreaFromAAVStep,
    SimulationAreaFromOSMStep,
]
