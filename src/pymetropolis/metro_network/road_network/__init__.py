from .capacities import ExogenousCapacitiesStep
from .circular import CircularNetworkStep
from .custom import CustomRoadImportStep
from .files import (
    RoadEdgesCapacitiesFile,
    RoadEdgesCleanFile,
    RoadEdgesRawFile,
    RoadEdgesUrbanFlagFile,
)
from .grid import GridNetworkStep
from .osm import OpenStreetMapRoadImportStep
from .postprocess import PostprocessRoadNetworkStep
from .urban import UrbanEdgesStep

ROAD_NETWORK_FILES = [
    RoadEdgesCleanFile,
    RoadEdgesCapacitiesFile,
    RoadEdgesRawFile,
    RoadEdgesUrbanFlagFile,
]

ROAD_NETWORK_STEPS = [
    CustomRoadImportStep,
    OpenStreetMapRoadImportStep,
    PostprocessRoadNetworkStep,
    ExogenousCapacitiesStep,
    CircularNetworkStep,
    GridNetworkStep,
    UrbanEdgesStep,
]
