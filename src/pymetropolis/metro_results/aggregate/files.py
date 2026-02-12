from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType


class IterationResultsFile(MetroDataFrameFile):
    path = "results/iteration_results.parquet"
    description = "Clean aggregate results over iterations."
    schema = [
        Column(
            "iteration",
            MetroDataType.UINT,
            description="Iteration counter.",
            nullable=False,
            unique=True,
        ),
        Column(
            "mean_surplus",
            MetroDataType.FLOAT,
            description="Mean surplus (or expected utility) over agents.",
            nullable=False,
        ),
        Column(
            "std_surplus",
            MetroDataType.FLOAT,
            description="Standard-deviation of surplus (or expected utility) over agents. This can be an indicator of equity.",
            nullable=False,
        ),
        Column(
            "mean_tour_departure_time",
            MetroDataType.TIME,
            description="Mean tour-level departure time.",
        ),
        Column(
            "mean_tour_arrival_time",
            MetroDataType.TIME,
            description="Mean tour-level arrival time.",
        ),
        Column(
            "mean_tour_travel_time",
            MetroDataType.DURATION,
            description="Mean tour-level travel time.",
        ),
        Column(
            "mean_tour_simulated_utility",
            MetroDataType.FLOAT,
            description="Mean tour-level simulated utility.",
        ),
        Column(
            "mean_tour_expected_utility",
            MetroDataType.FLOAT,
            description="Mean tour-level expected utility *of the selected mode*.",
        ),
        Column(
            "mean_tour_departure_time_shift",
            MetroDataType.DURATION,
            description="Mean tour-level departure-time shift compared to the previous iteration, for tours with no mode shift.",
        ),
        Column(
            "rmse_tour_departure_time",
            MetroDataType.DURATION,
            description="RMSE of tour-level departure-time shifts for tours with no mode shift.",
        ),
        Column(
            "nb_road_trips",
            MetroDataType.UINT,
            description="Total number of road trips.",
            nullable=False,
        ),
        Column(
            "nb_non_road_trips",
            MetroDataType.UINT,
            description="Total number of non-road trips.",
            nullable=False,
        ),
        Column(
            "nb_outside_options",
            MetroDataType.UINT,
            description="Number of tours choosing the outside option.",
            nullable=False,
        ),
        Column(
            "mean_trip_departure_time",
            MetroDataType.TIME,
            description="Mean departure time from origin of all trips.",
        ),
        Column(
            "mean_trip_arrival_time",
            MetroDataType.TIME,
            description="Mean arrival time at destination of all trips.",
        ),
        Column(
            "mean_trip_travel_time",
            MetroDataType.DURATION,
            description="Mean trip-level travel time of all trips.",
        ),
        Column(
            "mean_trip_utility",
            MetroDataType.FLOAT,
            description="Mean simulated utility of all trips.",
        ),
        Column(
            "mean_road_trip_departure_time",
            MetroDataType.TIME,
            description="Mean departure time from origin of road trips.",
        ),
        Column(
            "mean_road_trip_arrival_time",
            MetroDataType.TIME,
            description="Mean arrival time at destination of road trips.",
        ),
        Column(
            "mean_road_trip_travel_time",
            MetroDataType.DURATION,
            description="Mean trip-level travel time of road trips.",
        ),
        Column(
            "mean_road_trip_route_free_flow_travel_time_mean",
            MetroDataType.DURATION,
            description="Mean free-flow travel time of road trips, on the selected route.",
        ),
        Column(
            "mean_road_trip_global_free_flow_travel_time",
            MetroDataType.DURATION,
            description="Mean travel time of road trips, on the fastest free-flow route.",
        ),
        Column(
            "mean_road_trip_route_congestion_time",
            MetroDataType.DURATION,
            description="Mean time lost in congestion of road trips, for the selected route.",
        ),
        Column(
            "mean_road_trip_global_congestion_time",
            MetroDataType.DURATION,
            description="Mean time lost in congestion of road trips, compared to the fastest free-flow route.",
        ),
        Column(
            "mean_road_trip_length",
            MetroDataType.FLOAT,
            description="Mean length of the selected route for road trips (in meters).",
        ),
        Column(
            "mean_road_trip_edge_count",
            MetroDataType.FLOAT,
            description="Mean number of edges of the selected route for road trips.",
        ),
        Column(
            "mean_road_trip_utility",
            MetroDataType.FLOAT,
            description="Mean simulated utility of road trips.",
        ),
        Column(
            "mean_road_trip_exp_travel_time",
            MetroDataType.DURATION,
            description="Mean expected travel time of road trips.",
        ),
        Column(
            "mean_road_trip_exp_travel_time_abs_diff",
            MetroDataType.DURATION,
            description="Mean absolute difference between expected and simulated travel time of road trips.",
        ),
        Column(
            "rmse_road_trip_exp_travel_time_diff",
            MetroDataType.DURATION,
            description="RMSE of the difference between the expected and simulated travel time of road trips.",
        ),
        Column(
            "mean_road_trip_length_diff",
            MetroDataType.FLOAT,
            description="Mean length of the selected route that was not selected during the previous iteration, for road trips.",
        ),
        Column(
            "rmse_simulated_road_travel_times",
            MetroDataType.DURATION,
            description="RMSE between the simulated edge-level travel-time function for the current iteration and the expected edge-level travel-time function for the previous iteration. The mean is taken over all edges and vehicle types.",
        ),
        Column(
            "rmse_expected_road_travel_times",
            MetroDataType.DURATION,
            description="RMSE between the expected edge-level travel-time function for the current iteration and the expected edge-level travel-time function for the previous iteration. The mean is taken over all edges and vehicle types.",
        ),
    ]
