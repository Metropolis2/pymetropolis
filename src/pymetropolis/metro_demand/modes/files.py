from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType

OUTSIDE_OPTION_PARAMETERS_FILE = MetroDataFrameFile(
    "outside_option_parameters",
    path="demand/population/outside_option_parameters.parquet",
    description="TODO",
    schema=[
        Column(
            "tour_id",
            MetroDataType.ID,
            description="Identifier of the tour",
            unique=True,
            nullable=False,
        ),
        Column(
            "outside_option_cst",
            MetroDataType.FLOAT,
            description="Utility of the outside option (€).",
            nullable=True,
        ),
    ],
)

CAR_DRIVER_PARAMETERS_FILE = MetroDataFrameFile(
    "car_driver_parameters",
    path="demand/population/car_driver_parameters.parquet",
    description="TODO",
    schema=[
        Column(
            "person_id",
            MetroDataType.ID,
            description="Identifier of the person",
            unique=True,
            nullable=False,
        ),
        Column(
            "car_driver_cst",
            MetroDataType.FLOAT,
            description="Penalty for each trip as a car driver (€).",
            nullable=True,
        ),
        Column(
            "car_driver_vot",
            MetroDataType.FLOAT,
            description="Value of time as a car driver (€/h).",
            nullable=True,
        ),
    ],
)

CAR_DRIVER_ODS_FILE = MetroDataFrameFile(
    "car_driver_ods",
    path="demand/population/car_driver_origins_destinations.parquet",
    description="TODO",
    schema=[
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip",
            unique=True,
            nullable=False,
        ),
        Column(
            "origin_node_id",
            MetroDataType.ID,
            description="Identifier of the origin node",
            nullable=False,
        ),
        Column(
            "destination_node_id",
            MetroDataType.ID,
            description="Identifier of the destination node",
            nullable=False,
        ),
    ],
)

CAR_DRIVER_DISTANCES_FILE = MetroDataFrameFile(
    "car_driver_distances",
    path="demand/population/car_driver_distances.parquet",
    description="Shortest path distance on the road network of all car driver trips",
    schema=[
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip",
            unique=True,
            nullable=False,
        ),
        Column(
            "distance",
            MetroDataType.FLOAT,
            description="Distance of the shortest path, in meters",
            nullable=False,
        ),
    ],
)

PUBLIC_TRANSIT_PARAMETERS_FILE = MetroDataFrameFile(
    "public_transit_parameters",
    path="demand/population/public_transit_parameters.parquet",
    description="TODO",
    schema=[
        Column(
            "person_id",
            MetroDataType.ID,
            description="Identifier of the person",
            unique=True,
            nullable=False,
        ),
        Column(
            "public_transit_cst",
            MetroDataType.FLOAT,
            description="Penalty for each trip in public transit (€).",
            nullable=True,
        ),
        Column(
            "public_transit_vot",
            MetroDataType.FLOAT,
            description="Value of time in public transit (€/h).",
            nullable=True,
        ),
    ],
)

PUBLIC_TRANSIT_TRAVEL_TIMES_FILE = MetroDataFrameFile(
    "public_transit_travel_times",
    path="demand/population/public_transit_travel_times.parquet",
    description="TODO",
    schema=[
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip",
            unique=True,
            nullable=False,
        ),
        Column(
            "public_transit_travel_time",
            MetroDataType.DURATION,
            description="Duration of the trip by public transit",
            nullable=False,
        ),
    ],
)
