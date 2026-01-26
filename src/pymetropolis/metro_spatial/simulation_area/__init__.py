from .aav import SimulationAreaFromAAVStep
from .bbox import SimulationAreaFromBboxStep
from .file import SimulationAreaFile
from .osm import SimulationAreaFromOSMStep
from .polygons import SimulationAreaFromPolygonsStep

SIMULATION_AREA_FILES = [SimulationAreaFile]
SIMULATION_AREA_STEPS = [
    SimulationAreaFromBboxStep,
    SimulationAreaFromPolygonsStep,
    SimulationAreaFromAAVStep,
    SimulationAreaFromOSMStep,
]
