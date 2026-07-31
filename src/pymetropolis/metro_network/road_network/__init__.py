from .capacities import ExogenousCapacitiesStep
from .circular import CircularNetworkStep
from .custom import CustomRoadImportStep
from .files import (
    AllRoadDistancesFile,
    RoadEdgesCapacitiesFile,
    RoadEdgesCleanFile,
    RoadEdgesRawFile,
    RoadEdgesUrbanFlagFile,
)
from .grid import GridNetworkStep
from .osm import OpenStreetMapRoadImportStep
from .postprocess import PostprocessRoadNetworkStep
from .routing import AllRoadDistancesStep
from .urban import UrbanEdgesStep

ROAD_NETWORK_FILES = [
    RoadEdgesCleanFile,
    RoadEdgesCapacitiesFile,
    RoadEdgesRawFile,
    RoadEdgesUrbanFlagFile,
    AllRoadDistancesFile,
]

ROAD_NETWORK_STEPS = [
    CustomRoadImportStep,
    OpenStreetMapRoadImportStep,
    PostprocessRoadNetworkStep,
    ExogenousCapacitiesStep,
    CircularNetworkStep,
    GridNetworkStep,
    UrbanEdgesStep,
    AllRoadDistancesStep,
]
