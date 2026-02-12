from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType


class MetroIterationResultsFile(MetroDataFrameFile):
    path = "run/output/iteration_results.parquet"
    description = "Aggregate results over iterations from the Metropolis-Core simulation."
    schema = [
        Column(
            "iteration_counter",
            MetroDataType.UINT,
            description="Iteration counter",
            nullable=False,
        ),
        Column(
            "surplus_mean",
            MetroDataType.FLOAT,
            description="Mean surplus (or expected utility) over agents.",
            nullable=False,
        ),
        Column(
            "surplus_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of surplus (or expected utility) over agents.",
            nullable=False,
        ),
        Column(
            "surplus_min",
            MetroDataType.FLOAT,
            description="Minimum surplus (or expected utility) over agents.",
            nullable=False,
        ),
        Column(
            "surplus_max",
            MetroDataType.FLOAT,
            description="Maximum surplus (or expected utility) over agents.",
            nullable=False,
        ),
        Column(
            "trip_alt_count",
            MetroDataType.UINT,
            description="Number of agents who chose an alternative with at least 1 trip.",
            nullable=False,
        ),
        Column(
            "alt_departure_time_mean",
            MetroDataType.FLOAT,
            description="Mean departure time of the first trip, over agents (number of seconds since midnight).",
        ),
        Column(
            "alt_departure_time_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of departure time of the first trip, over agents (number of seconds since midnight).",
        ),
        Column(
            "alt_departure_time_min",
            MetroDataType.FLOAT,
            description="Minimum departure time of the first trip, over agents (number of seconds since midnight).",
        ),
        Column(
            "alt_departure_time_max",
            MetroDataType.FLOAT,
            description="Maximum departure time of the first trip, over agents (number of seconds since midnight).",
        ),
        Column(
            "alt_arrival_time_mean",
            MetroDataType.FLOAT,
            description="Mean arrival time of the last trip, over agents (in number of seconds since midnight).",
        ),
        Column(
            "alt_arrival_time_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of arrival time of the last trip, over agents (in number of seconds since midnight).",
        ),
        Column(
            "alt_arrival_time_min",
            MetroDataType.FLOAT,
            description="Minimum arrival time of the last trip, over agents (in number of seconds since midnight).",
        ),
        Column(
            "alt_arrival_time_max",
            MetroDataType.FLOAT,
            description="Maximum arrival time of the last trip, over agents (in number of seconds since midnight).",
        ),
        Column(
            "alt_travel_time_mean",
            MetroDataType.FLOAT,
            description="Mean total travel time (i.e., for all trips), over agents (in seconds).",
        ),
        Column(
            "alt_travel_time_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of total travel time (i.e., for all trips), over agents (in seconds).",
        ),
        Column(
            "alt_travel_time_min",
            MetroDataType.FLOAT,
            description="Minimum total travel time (i.e., for all trips), over agents (in seconds).",
        ),
        Column(
            "alt_travel_time_max",
            MetroDataType.FLOAT,
            description="Maximum total travel time (i.e., for all trips), over agents (in seconds).",
        ),
        Column(
            "alt_utility_mean",
            MetroDataType.FLOAT,
            description="Mean simulated utility of the selected alternative, over agents.",
        ),
        Column(
            "alt_utility_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of simulated utility of the selected alternative, over agents.",
        ),
        Column(
            "alt_utility_min",
            MetroDataType.FLOAT,
            description="Minimum simulated utility of the selected alternative, over agents.",
        ),
        Column(
            "alt_utility_max",
            MetroDataType.FLOAT,
            description="Maximum simulated utility of the selected alternative, over agents.",
        ),
        Column(
            "alt_expected_utility_mean",
            MetroDataType.FLOAT,
            description="Mean surplus (or expected utility) for the selected alternative, over agents.",
        ),
        Column(
            "alt_expected_utility_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of surplus (or expected utility) for the selected alternative, over agents.",
        ),
        Column(
            "alt_expected_utility_min",
            MetroDataType.FLOAT,
            description="Minimum surplus (or expected utility) for the selected alternative, over agents.",
        ),
        Column(
            "alt_expected_utility_max",
            MetroDataType.FLOAT,
            description="Maximum surplus (or expected utility) for the selected alternative, over agents.",
        ),
        Column(
            "alt_dep_time_shift_mean",
            MetroDataType.FLOAT,
            description="Mean departure-time shift of the first trip compared to the previous iteration, over agent keeping the same alternative (in seconds).",
        ),
        Column(
            "alt_dep_time_shift_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of departure-time shift of the first trip compared to the previous iteration, over agent keeping the same alternative (in seconds).",
        ),
        Column(
            "alt_dep_time_shift_min",
            MetroDataType.FLOAT,
            description="Minimum departure-time shift of the first trip compared to the previous iteration, over agent keeping the same alternative (in seconds).",
        ),
        Column(
            "alt_dep_time_shift_max",
            MetroDataType.FLOAT,
            description="Maximum departure-time shift of the first trip compared to the previous iteration, over agent keeping the same alternative (in seconds).",
        ),
        Column(
            "alt_dep_time_rmse",
            MetroDataType.FLOAT,
            description="RMSE of first-trip departure-time shift over agents keeping the same alternative.",
        ),
        Column(
            "road_trip_count",
            MetroDataType.UINT,
            description="Total number of road trips in the selected alternatives.",
        ),
        Column(
            "nb_agents_at_least_one_road_trip",
            MetroDataType.UINT,
            description="Number of agents with at least one road trip in their selected alternative.",
        ),
        Column(
            "nb_agents_all_road_trips",
            MetroDataType.UINT,
            description="Number of agents with only road trips in their selected alternative.",
        ),
        Column(
            "road_trip_count_by_agent_mean",
            MetroDataType.FLOAT,
            description="Mean number of road trips, over all agents with at least one road trip.",
        ),
        Column(
            "road_trip_count_by_agent_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of number of road trips, over all agents with at least one road trip.",
        ),
        Column(
            "road_trip_count_by_agent_min",
            MetroDataType.FLOAT,
            description="Minimum number of road trips, over all agents with at least one road trip.",
        ),
        Column(
            "road_trip_count_by_agent_max",
            MetroDataType.FLOAT,
            description="Maximum number of road trips, over all agents with at least one road trip.",
        ),
        Column(
            "road_trip_departure_time_mean",
            MetroDataType.FLOAT,
            description="Mean departure time from origin, over all road trips (in number of seconds after midnight).",
        ),
        Column(
            "road_trip_departure_time_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of departure time from origin, over all road trips (in number of seconds after midnight).",
        ),
        Column(
            "road_trip_departure_time_min",
            MetroDataType.FLOAT,
            description="Minimum departure time from origin, over all road trips (in number of seconds after midnight).",
        ),
        Column(
            "road_trip_departure_time_max",
            MetroDataType.FLOAT,
            description="Maximum departure time from origin, over all road trips (in number of seconds after midnight).",
        ),
        Column(
            "road_trip_arrival_time_mean",
            MetroDataType.FLOAT,
            description="Mean arrival time at destination, over all road trips (in number of seconds after midnight).",
        ),
        Column(
            "road_trip_arrival_time_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of arrival time at destination, over all road trips (in number of seconds after midnight).",
        ),
        Column(
            "road_trip_arrival_time_min",
            MetroDataType.FLOAT,
            description="Minimum arrival time at destination, over all road trips (in number of seconds after midnight).",
        ),
        Column(
            "road_trip_arrival_time_max",
            MetroDataType.FLOAT,
            description="Maximum arrival time at destination, over all road trips (in number of seconds after midnight).",
        ),
        Column(
            "road_trip_road_time_mean",
            MetroDataType.FLOAT,
            description="Mean time spent on the road segments (i.e., travel time excluding bottleneck delays), over all road trips (in seconds).",
        ),
        Column(
            "road_trip_road_time_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of time spent on the road segments (i.e., travel time excluding bottleneck delays), over all road trips (in seconds).",
        ),
        Column(
            "road_trip_road_time_min",
            MetroDataType.FLOAT,
            description="Minimum time spent on the road segments (i.e., travel time excluding bottleneck delays), over all road trips (in seconds).",
        ),
        Column(
            "road_trip_road_time_max",
            MetroDataType.FLOAT,
            description="Maximum time spent on the road segments (i.e., travel time excluding bottleneck delays), over all road trips (in seconds).",
        ),
        Column(
            "road_trip_in_bottleneck_time_mean",
            MetroDataType.FLOAT,
            description="Mean delay at entry bottlenecks, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_in_bottleneck_time_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of delay at entry bottlenecks, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_in_bottleneck_time_min",
            MetroDataType.FLOAT,
            description="Minimum delay at entry bottlenecks, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_in_bottleneck_time_max",
            MetroDataType.FLOAT,
            description="Maximum delay at entry bottlenecks, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_out_bottleneck_time_mean",
            MetroDataType.FLOAT,
            description="Mean delay exit bottlenecks, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_out_bottleneck_time_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of delay exit bottlenecks, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_out_bottleneck_time_min",
            MetroDataType.FLOAT,
            description="Minimum delay exit bottlenecks, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_out_bottleneck_time_max",
            MetroDataType.FLOAT,
            description="Maximum delay exit bottlenecks, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_travel_time_mean",
            MetroDataType.FLOAT,
            description="Mean trip travel time, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_travel_time_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of trip travel time, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_travel_time_min",
            MetroDataType.FLOAT,
            description="Minimum trip travel time, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_travel_time_max",
            MetroDataType.FLOAT,
            description="Maximum trip travel time, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_route_free_flow_travel_time_mean",
            MetroDataType.FLOAT,
            description="Mean free-flow travel time of the selected route, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_route_free_flow_travel_time_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of free-flow travel time of the selected route, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_route_free_flow_travel_time_min",
            MetroDataType.FLOAT,
            description="Minimum free-flow travel time of the selected route, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_route_free_flow_travel_time_max",
            MetroDataType.FLOAT,
            description="Maximum free-flow travel time of the selected route, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_global_free_flow_travel_time_mean",
            MetroDataType.FLOAT,
            description="Mean travel time of the fastest free-flow route, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_global_free_flow_travel_time_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of travel time of the fastest free-flow route, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_global_free_flow_travel_time_min",
            MetroDataType.FLOAT,
            description="Minimum travel time of the fastest free-flow route, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_global_free_flow_travel_time_max",
            MetroDataType.FLOAT,
            description="Maximum travel time of the fastest free-flow route, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_route_congestion_mean",
            MetroDataType.FLOAT,
            description="Mean share of extra time spent in congestion for the selected route, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_route_congestion_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of share of extra time spent in congestion for the selected route, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_route_congestion_min",
            MetroDataType.FLOAT,
            description="Minimum share of extra time spent in congestion for the selected route, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_route_congestion_max",
            MetroDataType.FLOAT,
            description="Maximum share of extra time spent in congestion for the selected route, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_global_congestion_mean",
            MetroDataType.FLOAT,
            description="Mean share of extra time spent in congestion compared to the fastest free-flow route, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_global_congestion_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of share of extra time spent in congestion compared to the fastest free-flow route, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_global_congestion_min",
            MetroDataType.FLOAT,
            description="Minimum share of extra time spent in congestion compared to the fastest free-flow route, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_global_congestion_max",
            MetroDataType.FLOAT,
            description="Maximum share of extra time spent in congestion compared to the fastest free-flow route, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_length_mean",
            MetroDataType.FLOAT,
            description="Mean length of the selected route, over all road trips (in meters).",
        ),
        Column(
            "road_trip_length_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of length of the selected route, over all road trips (in meters).",
        ),
        Column(
            "road_trip_length_min",
            MetroDataType.FLOAT,
            description="Minimum length of the selected route, over all road trips (in meters).",
        ),
        Column(
            "road_trip_length_max",
            MetroDataType.FLOAT,
            description="Maximum length of the selected route, over all road trips (in meters).",
        ),
        Column(
            "road_trip_edge_count_mean",
            MetroDataType.FLOAT,
            description="Mean number of edges of the selected route, over all trips.",
        ),
        Column(
            "road_trip_edge_count_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of number of edges of the selected route, over all trips.",
        ),
        Column(
            "road_trip_edge_count_min",
            MetroDataType.FLOAT,
            description="Minimum number of edges of the selected route, over all trips.",
        ),
        Column(
            "road_trip_edge_count_max",
            MetroDataType.FLOAT,
            description="Maximum number of edges of the selected route, over all trips.",
        ),
        Column(
            "road_trip_utility_mean",
            MetroDataType.FLOAT,
            description="Mean simulated utility, over all road trips.",
        ),
        Column(
            "road_trip_utility_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of simulated utility, over all road trips.",
        ),
        Column(
            "road_trip_utility_min",
            MetroDataType.FLOAT,
            description="Minimum simulated utility, over all road trips.",
        ),
        Column(
            "road_trip_utility_max",
            MetroDataType.FLOAT,
            description="Maximum simulated utility, over all road trips.",
        ),
        Column(
            "road_trip_exp_travel_time_mean",
            MetroDataType.FLOAT,
            description="Mean expected travel time when departing, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_exp_travel_time_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of expected travel time when departing, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_exp_travel_time_min",
            MetroDataType.FLOAT,
            description="Minimum expected travel time when departing, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_exp_travel_time_max",
            MetroDataType.FLOAT,
            description="Maximum expected travel time when departing, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_exp_travel_time_rel_diff_mean",
            MetroDataType.FLOAT,
            description="Mean relative difference between the expected and simulated travel time, over all road trips.",
        ),
        Column(
            "road_trip_exp_travel_time_rel_diff_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of relative difference between the expected and simulated travel time, over all road trips.",
        ),
        Column(
            "road_trip_exp_travel_time_rel_diff_min",
            MetroDataType.FLOAT,
            description="Minimum relative difference between the expected and simulated travel time, over all road trips.",
        ),
        Column(
            "road_trip_exp_travel_time_rel_diff_max",
            MetroDataType.FLOAT,
            description="Maximum relative difference between the expected and simulated travel time, over all road trips.",
        ),
        Column(
            "road_trip_exp_travel_time_abs_diff_mean",
            MetroDataType.FLOAT,
            description="Mean absolute difference between the expected and simulated travel time, over all trips (in seconds).",
        ),
        Column(
            "road_trip_exp_travel_time_abs_diff_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of absolute difference between the expected and simulated travel time, over all trips (in seconds).",
        ),
        Column(
            "road_trip_exp_travel_time_abs_diff_min",
            MetroDataType.FLOAT,
            description="Minimum absolute difference between the expected and simulated travel time, over all trips (in seconds).",
        ),
        Column(
            "road_trip_exp_travel_time_abs_diff_max",
            MetroDataType.FLOAT,
            description="Maximum absolute difference between the expected and simulated travel time, over all trips (in seconds).",
        ),
        Column(
            "road_trip_exp_travel_time_diff_rmse",
            MetroDataType.FLOAT,
            description="RMSE of the absolute difference between the expected and simulated travel time, over all road trips (in seconds).",
        ),
        Column(
            "road_trip_length_diff_mean",
            MetroDataType.FLOAT,
            description="Mean length of the selected route that was not selected during the previous iteration, over all road trips.",
        ),
        Column(
            "road_trip_length_diff_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of length of the selected route that was not selected during the previous iteration, over all road trips.",
        ),
        Column(
            "road_trip_length_diff_min",
            MetroDataType.FLOAT,
            description="Minimum length of the selected route that was not selected during the previous iteration, over all road trips.",
        ),
        Column(
            "road_trip_length_diff_max",
            MetroDataType.FLOAT,
            description="Maximum length of the selected route that was not selected during the previous iteration, over all road trips.",
        ),
        Column(
            "virtual_trip_count",
            MetroDataType.FLOAT,
            description="Total number of virtual trips in the selected alternatives.",
        ),
        Column(
            "nb_agents_at_least_one_virtual_trip",
            MetroDataType.FLOAT,
            description="Number of agents with at least one virtual trip in their selected alternative.",
        ),
        Column(
            "nb_agents_all_virtual_trips",
            MetroDataType.FLOAT,
            description="Number of agents with only virtual trips in their selected alternative.",
        ),
        Column(
            "virtual_trip_count_by_agent_mean",
            MetroDataType.FLOAT,
            description="Mean number of virtual trips, over all agents with at least one virtual trip.",
        ),
        Column(
            "virtual_trip_count_by_agent_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of number of virtual trips, over all agents with at least one virtual trip.",
        ),
        Column(
            "virtual_trip_count_by_agent_min",
            MetroDataType.FLOAT,
            description="Minimum number of virtual trips, over all agents with at least one virtual trip.",
        ),
        Column(
            "virtual_trip_count_by_agent_max",
            MetroDataType.FLOAT,
            description="Maximum number of virtual trips, over all agents with at least one virtual trip.",
        ),
        Column(
            "virtual_trip_departure_time_mean",
            MetroDataType.FLOAT,
            description="Mean departure time from origin, over all virtual trips (in number of seconds after midnight).",
        ),
        Column(
            "virtual_trip_departure_time_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of departure time from origin, over all virtual trips (in number of seconds after midnight).",
        ),
        Column(
            "virtual_trip_departure_time_min",
            MetroDataType.FLOAT,
            description="Minimum departure time from origin, over all virtual trips (in number of seconds after midnight).",
        ),
        Column(
            "virtual_trip_departure_time_max",
            MetroDataType.FLOAT,
            description="Maximum departure time from origin, over all virtual trips (in number of seconds after midnight).",
        ),
        Column(
            "virtual_trip_arrival_time_mean",
            MetroDataType.FLOAT,
            description="Mean arrival time at destination, over all virtual trips (in number of seconds after midnight).",
        ),
        Column(
            "virtual_trip_arrival_time_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of arrival time at destination, over all virtual trips (in number of seconds after midnight).",
        ),
        Column(
            "virtual_trip_arrival_time_min",
            MetroDataType.FLOAT,
            description="Minimum arrival time at destination, over all virtual trips (in number of seconds after midnight).",
        ),
        Column(
            "virtual_trip_arrival_time_max",
            MetroDataType.FLOAT,
            description="Maximum arrival time at destination, over all virtual trips (in number of seconds after midnight).",
        ),
        Column(
            "virtual_trip_travel_time_mean",
            MetroDataType.FLOAT,
            description="Mean travel time, over all virtual trips (in seconds).",
        ),
        Column(
            "virtual_trip_travel_time_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of travel time, over all virtual trips (in seconds).",
        ),
        Column(
            "virtual_trip_travel_time_min",
            MetroDataType.FLOAT,
            description="Minimum travel time, over all virtual trips (in seconds).",
        ),
        Column(
            "virtual_trip_travel_time_max",
            MetroDataType.FLOAT,
            description="Maximum travel time, over all virtual trips (in seconds).",
        ),
        Column(
            "virtual_trip_global_free_flow_travel_time_mean",
            MetroDataType.FLOAT,
            description="Mean of the smallest possible travel time, over all virtual trips (in seconds). Only relevant for time-dependent virtual trips. Only relevant for time-dependent virtual trips.",
        ),
        Column(
            "virtual_trip_global_free_flow_travel_time_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of of the smallest possible travel time, over all virtual trips (in seconds). Only relevant for time-dependent virtual trips. Only relevant for time-dependent virtual trips.",
        ),
        Column(
            "virtual_trip_global_free_flow_travel_time_min",
            MetroDataType.FLOAT,
            description="Minimum of the smallest possible travel time, over all virtual trips (in seconds). Only relevant for time-dependent virtual trips. Only relevant for time-dependent virtual trips.",
        ),
        Column(
            "virtual_trip_global_free_flow_travel_time_max",
            MetroDataType.FLOAT,
            description="Maximum of the smallest possible travel time, over all virtual trips (in seconds). Only relevant for time-dependent virtual trips. Only relevant for time-dependent virtual trips.",
        ),
        Column(
            "virtual_trip_global_congestion_mean",
            MetroDataType.FLOAT,
            description="Mean share of extra time spent in congestion compared to the smallest possible travel time, over all road trips. Only relevant for time-dependent virtual trips.",
        ),
        Column(
            "virtual_trip_global_congestion_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of share of extra time spent in congestion compared to the smallest possible travel time, over all road trips. Only relevant for time-dependent virtual trips.",
        ),
        Column(
            "virtual_trip_global_congestion_min",
            MetroDataType.FLOAT,
            description="Minimum share of extra time spent in congestion compared to the smallest possible travel time, over all road trips. Only relevant for time-dependent virtual trips.",
        ),
        Column(
            "virtual_trip_global_congestion_max",
            MetroDataType.FLOAT,
            description="Maximum share of extra time spent in congestion compared to the smallest possible travel time, over all road trips. Only relevant for time-dependent virtual trips.",
        ),
        Column(
            "virtual_trip_utility_mean",
            MetroDataType.FLOAT,
            description="Mean simulated utility, over all virtual trips.",
        ),
        Column(
            "virtual_trip_utility_std",
            MetroDataType.FLOAT,
            description="Standard-deviation of simulated utility, over all virtual trips.",
        ),
        Column(
            "virtual_trip_utility_min",
            MetroDataType.FLOAT,
            description="Minimum simulated utility, over all virtual trips.",
        ),
        Column(
            "virtual_trip_utility_max",
            MetroDataType.FLOAT,
            description="Maximum simulated utility, over all virtual trips.",
        ),
        Column(
            "no_trip_alt_count",
            MetroDataType.UINT,
            description="Number of agents with no trip in their selected alternative.",
            nullable=False,
        ),
        Column(
            "sim_road_network_cond_rmse",
            MetroDataType.FLOAT,
            description="RMSE between the simulated edge-level travel-time function for the current iteration and the expected edge-level travel-time function for the previous iteration. The mean is taken over all edges and vehicle types.",
        ),
        Column(
            "exp_road_network_cond_rmse",
            MetroDataType.FLOAT,
            description="RMSE between the expected edge-level travel-time function for the current iteration and the expected edge-level travel-time function for the previous iteration. The mean is taken over all edges and vehicle types.",
        ),
    ]


class MetroTripResultsFile(MetroDataFrameFile):
    path = "run/output/trip_results.parquet"
    description = "Trip-level results from the Metropolis-Core simulation."
    schema = [
        Column(
            "agent_id",
            MetroDataType.ID,
            description="Identifier of the agent performing the trip.",
            nullable=False,
        ),
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            nullable=False,
        ),
        Column(
            "trip_index",
            MetroDataType.UINT,
            description="Index of the trip in the agent's trip chain.",
            nullable=False,
        ),
        Column(
            "departure_time",
            MetroDataType.FLOAT,
            description="Departure time of the trip, in seconds after midnight.",
            nullable=False,
        ),
        Column(
            "arrival_time",
            MetroDataType.FLOAT,
            description="Arrival time of the trip, in seconds after midnight.",
            nullable=False,
        ),
        Column(
            "travel_utility",
            MetroDataType.FLOAT,
            description="Travel utility of the trip.",
            nullable=False,
        ),
        Column(
            "schedule_utility",
            MetroDataType.FLOAT,
            description="Schedule utility of the trip.",
            nullable=False,
        ),
        Column(
            "departure_time_shift",
            MetroDataType.FLOAT,
            description="By how much departure time changed compared to the previous iteration, in seconds.",
            nullable=False,
        ),
        Column(
            "road_time",
            MetroDataType.FLOAT,
            description="Time spent traveling on the road segments, in seconds.",
            nullable=True,
        ),
        Column(
            "in_bottleneck_time",
            MetroDataType.FLOAT,
            description="Time spent waiting at an entry bottleneck, in seconds.",
            nullable=True,
        ),
        Column(
            "out_bottleneck_time",
            MetroDataType.FLOAT,
            description="Time spent waiting at an exit bottleneck, in seconds.",
            nullable=True,
        ),
        Column(
            "route_free_flow_travel_time",
            MetroDataType.FLOAT,
            description="Free flow travel time of the trip, on the same route, in seconds.",
            nullable=True,
        ),
        Column(
            "global_free_flow_travel_time",
            MetroDataType.FLOAT,
            description="Free flow travel time of the trip, over any route, in seconds.",
            nullable=True,
        ),
        Column(
            "length",
            MetroDataType.FLOAT,
            description="Length of the route taken, in meters.",
            nullable=True,
        ),
        Column(
            "length_diff",
            MetroDataType.FLOAT,
            description="Length of the route taken that was not taken during the previous iteration, in meters.",
            nullable=True,
        ),
        Column(
            "pre_exp_departure_time",
            MetroDataType.FLOAT,
            description="Expected departure time of the trip before the iteration started, in seconds after midnight.",
            nullable=False,
        ),
        Column(
            "pre_exp_arrival_time",
            MetroDataType.FLOAT,
            description="Expected arrival time of the trip before the iteration started, in seconds after midnight.",
            nullable=False,
        ),
        Column(
            "exp_arrival_time",
            MetroDataType.FLOAT,
            description="Expected arrival time of the trip at trip start, in seconds after midnight.",
            nullable=False,
        ),
        Column(
            "nb_edges",
            MetroDataType.UINT,
            description="Number of road edges taken.",
            nullable=True,
        ),
    ]


class MetroAgentResultsFile(MetroDataFrameFile):
    path = "run/output/agent_results.parquet"
    description = "Agent-level results from the Metropolis-Core Simulation."
    schema = [
        Column(
            "agent_id",
            MetroDataType.ID,
            description="Identifier of the agent.",
            nullable=False,
        ),
        Column(
            "selected_alt_id",
            MetroDataType.ID,
            description="Identifier of the alternative chosen.",
            nullable=False,
        ),
        Column(
            "expected_utility",
            MetroDataType.FLOAT,
            description="Expected utility of the agent.",
            nullable=False,
        ),
        Column(
            "shifted_alt",
            MetroDataType.BOOL,
            description="Whether the agent shifted chosen alternative compared to the previous iteration.",
            nullable=False,
        ),
        Column(
            "departure_time",
            MetroDataType.FLOAT,
            description="Departure time of the trip, in seconds after midnight.",
            nullable=True,
        ),
        Column(
            "arrival_time",
            MetroDataType.FLOAT,
            description="Arrival time of the trip, in seconds after midnight.",
            nullable=True,
        ),
        Column(
            "total_travel_time",
            MetroDataType.FLOAT,
            description="Total travel time spent over all the trips of the agent, in seconds.",
            nullable=True,
        ),
        Column(
            "utility",
            MetroDataType.FLOAT,
            description="Realized utility of the agent.",
            nullable=False,
        ),
        Column(
            "alt_expected_utility",
            MetroDataType.FLOAT,
            description="Expected utility of the agent for the chosen alternative.",
            nullable=False,
        ),
        Column(
            "departure_time_shift",
            MetroDataType.FLOAT,
            description="By how much departure time changed compared to the previous iteration, in seconds.",
            nullable=True,
        ),
        Column(
            "nb_road_trips",
            MetroDataType.UINT,
            description="Number of road trips taken.",
            nullable=False,
        ),
        Column(
            "nb_virtual_trips",
            MetroDataType.UINT,
            description="Number of virtual trips taken.",
            nullable=False,
        ),
    ]


class MetroSimulatedTravelTimeFunctionsFile(MetroDataFrameFile):
    path = "run/output/net_cond_sim_edge_ttfs.parquet"
    description = (
        "Simulated travel time functions of the road-network edges for the last iteration, "
        "represented as a list of breakpoints."
    )
    schema = [
        Column(
            "vehicle_id",
            MetroDataType.ID,
            description="Identifier of the vehicle type.",
            nullable=False,
        ),
        Column(
            "edge_id",
            MetroDataType.ID,
            description="Identifier of the edge.",
            nullable=False,
        ),
        Column(
            "departure_time",
            MetroDataType.FLOAT,
            description="Departure time of the breakpoint, in number of seconds after midnight.",
            nullable=False,
        ),
        Column(
            "travel_time",
            MetroDataType.FLOAT,
            description="Travel time of the breakpoint, in number of seconds.",
            nullable=False,
        ),
    ]


class MetroExpectedTravelTimeFunctionsFile(MetroDataFrameFile):
    path = "run/output/net_cond_exp_edge_ttfs.parquet"
    description = (
        "Expected travel time functions of the road-network edges for the last iteration, "
        "represented as a list of breakpoints."
    )
    schema = [
        Column(
            "vehicle_id",
            MetroDataType.ID,
            description="Identifier of the vehicle type.",
            nullable=False,
        ),
        Column(
            "edge_id",
            MetroDataType.ID,
            description="Identifier of the edge.",
            nullable=False,
        ),
        Column(
            "departure_time",
            MetroDataType.FLOAT,
            description="Departure time of the breakpoint, in number of seconds after midnight.",
            nullable=False,
        ),
        Column(
            "travel_time",
            MetroDataType.FLOAT,
            description="Travel time of the breakpoint, in number of seconds.",
            nullable=False,
        ),
    ]


class MetroNextExpectedTravelTimeFunctionsFile(MetroDataFrameFile):
    path = "run/output/net_cond_next_exp_edge_ttfs.parquet"
    description = (
        "Expected travel time functions of the road-network edges for the next iteration, "
        "represented as a list of breakpoints."
    )
    schema = [
        Column(
            "vehicle_id",
            MetroDataType.ID,
            description="Identifier of the vehicle type.",
            nullable=False,
        ),
        Column(
            "edge_id",
            MetroDataType.ID,
            description="Identifier of the edge.",
            nullable=False,
        ),
        Column(
            "departure_time",
            MetroDataType.FLOAT,
            description="Departure time of the breakpoint, in number of seconds after midnight.",
            nullable=False,
        ),
        Column(
            "travel_time",
            MetroDataType.FLOAT,
            description="Travel time of the breakpoint, in number of seconds.",
            nullable=False,
        ),
    ]
