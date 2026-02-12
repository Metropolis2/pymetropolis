from .files import (
    ExpectedRoadNetworkCongestionFunctionPlotFile,
    ExpectedRoadTravelTimesConvergencePlotFile,
    SimulatedRoadTravelTimesConvergencePlotFile,
    SimulationRoadNetworkCongestionFunctionPlotFile,
    TourDepartureTimeConvergencePlotFile,
    TripDepartureTimeDistributionPlotFile,
)
from .steps import (
    ConvergencePlotStep,
    RoadNetworkCongestionFunctionPlotsStep,
    TripDepartureTimeDistributionStep,
)

PLOTS_FILES = [
    TourDepartureTimeConvergencePlotFile,
    SimulatedRoadTravelTimesConvergencePlotFile,
    ExpectedRoadTravelTimesConvergencePlotFile,
    TripDepartureTimeDistributionPlotFile,
    ExpectedRoadNetworkCongestionFunctionPlotFile,
    SimulationRoadNetworkCongestionFunctionPlotFile,
]
PLOTS_STEPS = [
    ConvergencePlotStep,
    TripDepartureTimeDistributionStep,
    RoadNetworkCongestionFunctionPlotsStep,
]
