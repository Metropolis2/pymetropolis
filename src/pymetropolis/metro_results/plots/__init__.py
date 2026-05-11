from .files import (
    ExpectedRoadNetworkCongestionFunctionPlotFile,
    ExpectedRoadTravelTimesConvergencePlotFile,
    MeanSurplusConvergencePlotFile,
    RouteLengthDiffConvergencePlotFile,
    SimulatedRoadTravelTimesConvergencePlotFile,
    SimulationRoadNetworkCongestionFunctionPlotFile,
    TourDepartureTimeConvergencePlotFile,
    TripDepartureTimeDistributionPlotFile,
    TripModeSharesPlotFile,
)
from .steps import (
    ConvergencePlotStep,
    RoadNetworkCongestionFunctionPlotsStep,
    TripDepartureTimeDistributionStep,
    TripModeSharesStep,
)

PLOTS_FILES = [
    TourDepartureTimeConvergencePlotFile,
    SimulatedRoadTravelTimesConvergencePlotFile,
    ExpectedRoadTravelTimesConvergencePlotFile,
    RouteLengthDiffConvergencePlotFile,
    MeanSurplusConvergencePlotFile,
    TripDepartureTimeDistributionPlotFile,
    ExpectedRoadNetworkCongestionFunctionPlotFile,
    SimulationRoadNetworkCongestionFunctionPlotFile,
    TripModeSharesPlotFile,
]
PLOTS_STEPS = [
    ConvergencePlotStep,
    TripDepartureTimeDistributionStep,
    RoadNetworkCongestionFunctionPlotsStep,
    TripModeSharesStep,
]
