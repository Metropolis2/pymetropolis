from .files import (
    ExpectedRoadNetworkCongestionFunctionPlotFile,
    ExpectedRoadTravelTimesConvergencePlotFile,
    MeanSurplusConvergencePlotFile,
    RoadTripsShareConvergencePlotFile,
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
    RoadTripsShareConvergencePlotFile,
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
