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
    EdgesPenaltiesFile,
    RawEdgesFile,
)
from .penalties import ExogenousEdgePenaltiesStep
from .postprocess import PostprocessRoadNetworkStep
from .routing import AllFreeFlowTravelTimesStep, AllRoadDistancesStep

# from .osm import OSMRoadImportStep

ROAD_NETWORK_FILES = [
    AllFreeFlowTravelTimesFile,
    CleanEdgesFile,
    EdgesCapacitiesFile,
    EdgesPenaltiesFile,
    RawEdgesFile,
    AllRoadDistancesFile,
]

ROAD_NETWORK_STEPS = [
    CustomRoadImportStep,
    # OSMRoadImportStep,
    PostprocessRoadNetworkStep,
    ExogenousCapacitiesStep,
    CircularNetworkStep,
    AllFreeFlowTravelTimesStep,
    AllRoadDistancesStep,
    ExogenousEdgePenaltiesStep,
]
