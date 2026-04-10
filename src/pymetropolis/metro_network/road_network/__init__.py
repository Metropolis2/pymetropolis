from .capacities import ExogenousCapacitiesStep
from .circular import CircularNetworkStep
from .custom import CustomRoadImportStep
from .files import (
    AllRoadDistancesFile,
    AllRoadFreeFlowTravelTimesFile,
    RoadEdgesCapacitiesFile,
    RoadEdgesCleanFile,
    RoadEdgesFreeFlowTravelTimeFile,
    RoadEdgesPenaltiesFile,
    RoadEdgesRawFile,
    RoadEdgesUrbanFlagFile,
)
from .grid import GridNetworkStep
from .osm import OpenStreetMapRoadImportStep
from .penalties import EdgesFreeFlowTravelTimesStep, ExogenousEdgePenaltiesStep
from .postprocess import PostprocessRoadNetworkStep
from .routing import AllFreeFlowTravelTimesStep, AllRoadDistancesStep
from .urban import UrbanEdgesStep

ROAD_NETWORK_FILES = [
    AllRoadFreeFlowTravelTimesFile,
    RoadEdgesCleanFile,
    RoadEdgesCapacitiesFile,
    RoadEdgesPenaltiesFile,
    RoadEdgesFreeFlowTravelTimeFile,
    RoadEdgesRawFile,
    AllRoadDistancesFile,
    RoadEdgesUrbanFlagFile,
]

ROAD_NETWORK_STEPS = [
    CustomRoadImportStep,
    OpenStreetMapRoadImportStep,
    PostprocessRoadNetworkStep,
    ExogenousCapacitiesStep,
    CircularNetworkStep,
    GridNetworkStep,
    AllFreeFlowTravelTimesStep,
    AllRoadDistancesStep,
    ExogenousEdgePenaltiesStep,
    EdgesFreeFlowTravelTimesStep,
    UrbanEdgesStep,
]
