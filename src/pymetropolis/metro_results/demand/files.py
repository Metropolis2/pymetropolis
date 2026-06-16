from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType


class TripResultsFile(MetroDataFrameFile):
    path = "results/trip_results.parquet"
    description = "Clean results for each trip."
    schema = [
        Column("trip_id", MetroDataType.ID, description="Identifier of the trip.", nullable=False),
        Column("mode", MetroDataType.STRING, description="Mode used for the trip.", nullable=False),
        Column(
            "vehicle_id",
            MetroDataType.ID,
            description="Vehicle type used for the trip.",
            nullable=True,
        ),
        Column(
            "is_road",
            MetroDataType.BOOL,
            description="Whether the trip is done on the road network.",
            nullable=False,
        ),
        Column(
            "departure_time",
            MetroDataType.DURATION,
            description="Departure time of the trip.",
            nullable=False,
        ),
        Column(
            "arrival_time",
            MetroDataType.DURATION,
            description="Arrival time of the trip.",
            nullable=False,
        ),
        Column(
            "travel_time",
            MetroDataType.DURATION,
            description="Travel time of the trip.",
            nullable=False,
        ),
        Column(
            "route_free_flow_travel_time",
            MetroDataType.DURATION,
            description="Free flow travel time of the trip, on the same route.",
            nullable=True,
        ),
        Column(
            "global_free_flow_travel_time",
            MetroDataType.DURATION,
            description="Free flow travel time of the trip, over any route.",
            nullable=True,
        ),
        Column("utility", MetroDataType.FLOAT, description="Utility of the trip.", nullable=True),
        Column(
            "travel_utility",
            MetroDataType.FLOAT,
            description="Travel utility of the trip.",
            nullable=True,
        ),
        Column(
            "schedule_utility",
            MetroDataType.FLOAT,
            description="Schedule utility of the trip.",
            nullable=True,
        ),
        Column(
            "route_length",
            MetroDataType.FLOAT,
            description="Length of the route taken, in meters.",
            nullable=True,
        ),
        Column(
            "nb_edges", MetroDataType.UINT, description="Number of road edges taken.", nullable=True
        ),
    ]


class RouteResultsFile(MetroDataFrameFile):
    path = "results/route_results.parquet"
    description = "Clean route results for each road trip."
    schema = [
        Column("trip_id", MetroDataType.ID, description="Identifier of the trip.", nullable=False),
        Column(
            "edge_id", MetroDataType.ID, description="Identifier of the edge taken.", nullable=False
        ),
        Column(
            "entry_time",
            MetroDataType.DURATION,
            description="Entry time on the edge.",
            nullable=False,
        ),
        Column(
            "exit_time",
            MetroDataType.DURATION,
            description="Exit time on the edge.",
            nullable=False,
        ),
        Column(
            "travel_time",
            MetroDataType.DURATION,
            description="Time spent on the edge.",
            nullable=False,
        ),
    ]


class ActivityResultsFile(MetroDataFrameFile):
    path = "results/activity_results.parquet"
    description = "Clean results for each activity."
    schema = [
        Column(
            "person_id", MetroDataType.ID, description="Identifier of the person.", nullable=False
        ),
        Column(
            "preceding_trip_id",
            MetroDataType.ID,
            description="Identifier of the trip before the activity.",
            nullable=True,
        ),
        Column(
            "following_trip_id",
            MetroDataType.ID,
            description="Identifier of the trip after the activity.",
            nullable=True,
        ),
        Column(
            "purpose", MetroDataType.STRING, description="Purpose of the activity.", nullable=True
        ),
        Column(
            "start_time",
            MetroDataType.DURATION,
            description="Start time of the activity.",
            nullable=True,
        ),
        Column(
            "end_time",
            MetroDataType.DURATION,
            description="End time of the activity.",
            nullable=True,
        ),
        Column(
            "activity_duration",
            MetroDataType.DURATION,
            description="Duration of the activity.",
            nullable=True,
        ),
    ]
