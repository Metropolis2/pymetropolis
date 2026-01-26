from .capacities import ExogenousCapacitiesStep
from .circular import CircularNetworkStep
from .custom import CustomRoadImportStep
from .files import (
    AllDistancesFile,
    CleanEdgesFile,
    EdgesCapacitiesFile,
    EdgesPenaltiesFile,
    RawEdgesFile,
)
from .files import (
    AllFreeFlowTravelTimesFile as AllFreeFlowTravelTimesFile,
)
from .penalties import ExogenousEdgePenaltiesStep
from .postprocess import PostprocessRoadNetworkStep
from .routing import AllDistancesStep, AllFreeFlowTravelTimesStep

# from .osm import OSMRoadImportStep

ROAD_NETWORK_FILES = [
    AllFreeFlowTravelTimesFile,
    CleanEdgesFile,
    EdgesCapacitiesFile,
    EdgesPenaltiesFile,
    RawEdgesFile,
    AllDistancesFile,
]

ROAD_NETWORK_STEPS = [
    CustomRoadImportStep,
    # OSMRoadImportStep,
    PostprocessRoadNetworkStep,
    ExogenousCapacitiesStep,
    CircularNetworkStep,
    AllFreeFlowTravelTimesStep,
    AllDistancesStep,
    ExogenousEdgePenaltiesStep,
]
