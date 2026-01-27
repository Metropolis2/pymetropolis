from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType


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
