from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType


class OutsideOptionPreferencesFile(MetroDataFrameFile):
    path = "demand/population/outside_option_preferences.parquet"
    description = "Utility of the outside option alternative, for each tour."
    schema = [
        Column(
            "tour_id",
            MetroDataType.ID,
            description="Identifier of the tour.",
            unique=True,
            nullable=False,
        ),
        Column(
            "outside_option_cst",
            MetroDataType.FLOAT,
            description="Utility of the outside option (€).",
            nullable=True,
        ),
    ]


class OutsideOptionTravelTimesFile(MetroDataFrameFile):
    path = "demand/population/outside_option_travel_times.parquet"
    description = "Travel time of the outside option alternative, for each tour."
    schema = [
        Column(
            "tour_id",
            MetroDataType.ID,
            description="Identifier of the tour.",
            unique=True,
            nullable=False,
        ),
        Column(
            "outside_option_travel_time",
            MetroDataType.DURATION,
            description="Duration of the tour for the outside option.",
            nullable=False,
        ),
    ]


class CarDriverPreferencesFile(MetroDataFrameFile):
    path = "demand/population/car_driver_preferences.parquet"
    description = "Preferences to travel as a car driver, for each person."
    schema = [
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
    ]


class CarDriverODsFile(MetroDataFrameFile):
    path = "demand/population/car_driver_origins_destinations.parquet"
    description = (
        "Origin / destination on the road network for each trip, when traveling as a car driver."
    )
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "origin_node_id",
            MetroDataType.ID,
            description="Identifier of the origin node, on the road network.",
            nullable=False,
        ),
        Column(
            "destination_node_id",
            MetroDataType.ID,
            description="Identifier of the destination node, on the road network.",
            nullable=False,
        ),
    ]


class CarDriverDistancesFile(MetroDataFrameFile):
    path = "demand/population/car_driver_distances.parquet"
    description = "Shortest path distance on the road network of each car driver trip."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "distance",
            MetroDataType.FLOAT,
            description="Distance of the shortest path, in meters.",
            nullable=False,
        ),
    ]


class PublicTransitPreferencesFile(MetroDataFrameFile):
    path = "demand/population/public_transit_preferences.parquet"
    description = "Preferences to travel by public transit, for each person."
    schema = [
        Column(
            "person_id",
            MetroDataType.ID,
            description="Identifier of the person.",
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
    ]


class PublicTransitTravelTimesFile(MetroDataFrameFile):
    path = "demand/population/public_transit_travel_times.parquet"
    description = "Travel time of each trip, when traveling by public transit."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "public_transit_travel_time",
            MetroDataType.DURATION,
            description="Duration of the trip by public transit.",
            nullable=False,
        ),
    ]
