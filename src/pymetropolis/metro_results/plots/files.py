from pymetropolis.metro_pipeline.file import MetroPlotFile


class TourDepartureTimeConvergencePlotFile(MetroPlotFile):
    path = "results/graphs/convergence_tour_departure_time.png"
    description = "RMSE of departure-time shift from one iteration to another."


class SimulatedRoadTravelTimesConvergencePlotFile(MetroPlotFile):
    path = "results/graphs/convergence_simulated_road_travel_times.png"
    description = "RMSE of simulated edge-level travel times from one iteration to another."


class ExpectedRoadTravelTimesConvergencePlotFile(MetroPlotFile):
    path = "results/graphs/convergence_expected_road_travel_times.png"
    description = "RMSE of expected edge-level travel times from one iteration to another."


class TripDepartureTimeDistributionPlotFile(MetroPlotFile):
    path = "results/graphs/trip_departure_time_distribution.png"
    description = "Histogram of departure time distribution, over trips."


class ExpectedRoadNetworkCongestionFunctionPlotFile(MetroPlotFile):
    path = "results/graphs/network_congestion_function_expected.png"
    description = (
        "Expected congestion function over all edges of the road network. "
        "Values are computed as `Σ exp travel time / Σ free-flow travel time - 1`, "
        "with the sumations over all edges."
    )


class SimulationRoadNetworkCongestionFunctionPlotFile(MetroPlotFile):
    path = "results/graphs/network_congestion_function_simulated.png"
    description = (
        "Simulated congestion function over all edges of the road network. "
        "Values are computed as `Σ sim travel time / Σ free-flow travel time - 1`, "
        "with the sumations over all edges."
    )
