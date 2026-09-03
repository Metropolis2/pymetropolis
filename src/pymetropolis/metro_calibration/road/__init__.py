from .congestion import CongestionSimulationStep
from .files import (
    AllRoadFreeFlowTravelTimesFile,
    FreeFlowTravelTimeComparisonPlotFile,
    RoadEdgesFreeFlowTravelTimeFile,
    RoadEdgesPenaltiesFile,
    RoadEdgesPenaltyCoefficientsFile,
    RoadEdgesVariablesFile,
    TomTomCongestionTimesFile,
    TomTomRoutesFile,
    TomTomRoutesMatchedFile,
)
from .free_flow_lasso import FreeFlowLassoStep, FreeFlowTravelTimeComparisonStep
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
    FreeFlowTravelTimeComparisonPlotFile,
    TomTomCongestionTimesFile,
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
    FreeFlowTravelTimeComparisonStep,
    CongestionSimulationStep,
]
