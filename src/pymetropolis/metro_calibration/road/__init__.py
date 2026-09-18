from .congestion import CongestionSimulationStep, CongestionTimeComparisonStep
from .files import (
    AllRoadFreeFlowTravelTimesFile,
    CongestionTimeComparisonPlotFile,
    FreeFlowTravelTimeComparisonPlotFile,
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
    AllRoadFreeFlowTravelTimesFile,
    TomTomRoutesFile,
    TomTomRoutesMatchedFile,
    FreeFlowTravelTimeComparisonPlotFile,
    TomTomCongestionTimesFile,
    CongestionTimeComparisonPlotFile,
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
    CongestionTimeComparisonStep,
]
