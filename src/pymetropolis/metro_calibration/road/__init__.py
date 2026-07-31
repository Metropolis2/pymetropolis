from .files import (
    AllRoadDistancesFile,
    AllRoadFreeFlowTravelTimesFile,
    RoadEdgesFreeFlowTravelTimeFile,
    RoadEdgesPenaltiesFile,
    RoadEdgesVariablesFile,
    TomTomRoutesFile,
    TomTomRoutesMatchedFile,
)
from .map_matching import MapMatchingStep
from .penalties import (
    EdgePenaltiesFromCoefficientsStep,
    EdgesFreeFlowTravelTimesStep,
    ExogenousEdgePenaltiesStep,
)
from .routing import AllFreeFlowTravelTimesStep, AllRoadDistancesStep
from .tomtom import TomTomRequestsStep
from .variables import RoadEdgesVariablesStep

ROAD_FILES = [
    RoadEdgesVariablesFile,
    RoadEdgesPenaltiesFile,
    RoadEdgesFreeFlowTravelTimeFile,
    AllRoadFreeFlowTravelTimesFile,
    AllRoadDistancesFile,
    TomTomRoutesFile,
    TomTomRoutesMatchedFile,
]

ROAD_STEPS = [
    RoadEdgesVariablesStep,
    ExogenousEdgePenaltiesStep,
    EdgesFreeFlowTravelTimesStep,
    EdgePenaltiesFromCoefficientsStep,
    AllFreeFlowTravelTimesStep,
    AllRoadDistancesStep,
    TomTomRequestsStep,
    MapMatchingStep,
]
