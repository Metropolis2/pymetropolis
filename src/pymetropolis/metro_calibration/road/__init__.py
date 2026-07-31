from .files import (
    AllRoadFreeFlowTravelTimesFile,
    RoadEdgesFreeFlowTravelTimeFile,
    RoadEdgesPenaltiesFile,
    RoadEdgesPenaltyCoefficientsFile,
    RoadEdgesVariablesFile,
    TomTomRoutesFile,
    TomTomRoutesMatchedFile,
)
from .free_flow_lasso import FreeFlowLassoStep
from .map_matching import MapMatchingStep
from .penalties import (
    EdgePenaltiesFromCoefficientsStep,
    EdgesFreeFlowTravelTimesStep,
    ExogenousEdgePenaltiesStep,
)
from .routing import AllFreeFlowTravelTimesStep
from .tomtom import TomTomRequestsStep
from .variables import RoadEdgesVariablesStep

ROAD_FILES = [
    RoadEdgesVariablesFile,
    RoadEdgesPenaltyCoefficientsFile,
    RoadEdgesPenaltiesFile,
    RoadEdgesFreeFlowTravelTimeFile,
    AllRoadFreeFlowTravelTimesFile,
    TomTomRoutesFile,
    TomTomRoutesMatchedFile,
]

ROAD_STEPS = [
    RoadEdgesVariablesStep,
    ExogenousEdgePenaltiesStep,
    EdgesFreeFlowTravelTimesStep,
    EdgePenaltiesFromCoefficientsStep,
    AllFreeFlowTravelTimesStep,
    TomTomRequestsStep,
    MapMatchingStep,
    FreeFlowLassoStep,
]
