from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType

TRIP_RESULTS_FILE = MetroDataFrameFile(
    "trip_results",
    path="results/trip_results.parquet",
    description="TODO",
    schema=[
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip",
            nullable=False,
        ),
        Column(
            "mode",
            MetroDataType.STRING,
            description="Mode used for the trip",
            nullable=False,
        ),
        Column(
            "is_road",
            MetroDataType.BOOL,
            description="Whether the trip is done on the road network",
            nullable=False,
        ),
        Column(
            "departure_time",
            MetroDataType.TIME,
            description="Departure time of the trip",
            nullable=False,
        ),
        Column(
            "arrival_time",
            MetroDataType.TIME,
            description="Arrival time of the trip",
            nullable=False,
        ),
        Column(
            "travel_time",
            MetroDataType.DURATION,
            description="Travel time of the trip",
            nullable=False,
        ),
        Column(
            "route_free_flow_travel_time",
            MetroDataType.DURATION,
            description="Free flow travel time of the trip, on the same route",
            nullable=True,
        ),
        Column(
            "global_free_flow_travel_time",
            MetroDataType.DURATION,
            description="Free flow travel time of the trip, over any route",
            nullable=True,
        ),
        Column(
            "utility",
            MetroDataType.FLOAT,
            description="Utility of the trip",
            nullable=True,
        ),
        Column(
            "travel_utility",
            MetroDataType.FLOAT,
            description="Travel utility of the trip",
            nullable=True,
        ),
        Column(
            "schedule_utility",
            MetroDataType.FLOAT,
            description="Schedule utility of the trip",
            nullable=True,
        ),
        Column(
            "length",
            MetroDataType.FLOAT,
            description="Length of the route taken, in meters",
            nullable=True,
        ),
        Column(
            "nb_edges",
            MetroDataType.UINT,
            description="Number of road edges taken",
            nullable=True,
        ),
    ],
)
