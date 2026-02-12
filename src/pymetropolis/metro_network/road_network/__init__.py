from .capacities import ExogenousCapacitiesStep
from .circular import CircularNetworkStep
from .custom import CustomRoadImportStep
from .files import (
    AllFreeFlowTravelTimesFile as AllFreeFlowTravelTimesFile,
)
from .files import (
    AllRoadDistancesFile,
    CleanEdgesFile,
    EdgesCapacitiesFile,
    EdgesFreeFlowTravelTimeFile,
    EdgesPenaltiesFile,
    RawEdgesFile,
)
from .grid import GridNetworkStep
from .penalties import EdgesFreeFlowTravelTimesStep, ExogenousEdgePenaltiesStep
from .postprocess import PostprocessRoadNetworkStep
from .routing import AllFreeFlowTravelTimesStep, AllRoadDistancesStep

# from .osm import OSMRoadImportStep

ROAD_NETWORK_FILES = [
    AllFreeFlowTravelTimesFile,
    CleanEdgesFile,
    EdgesCapacitiesFile,
    EdgesPenaltiesFile,
    EdgesFreeFlowTravelTimeFile,
    RawEdgesFile,
    AllRoadDistancesFile,
]

ROAD_NETWORK_STEPS = [
    CustomRoadImportStep,
    # OSMRoadImportStep,
    PostprocessRoadNetworkStep,
    ExogenousCapacitiesStep,
    CircularNetworkStep,
    GridNetworkStep,
    AllFreeFlowTravelTimesStep,
    AllRoadDistancesStep,
    ExogenousEdgePenaltiesStep,
    EdgesFreeFlowTravelTimesStep,
]
