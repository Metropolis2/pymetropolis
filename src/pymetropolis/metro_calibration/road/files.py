from pymetropolis.metro_pipeline.file import (
    Column,
    MetroDataFrameFile,
    MetroDataType,
    MetroGeoDataFrameFile,
)


class RoadEdgesVariablesFile(MetroDataFrameFile):
    path = "calibration/road/edges_variables.parquet"
    description = (
        "Edge-level variables available for calibration. Defined columns depend on the "
        "configuration."
    )
    discard_extra_columns = False
    schema = [
        Column(
            "edge_id",
            MetroDataType.ID,
            description="Identifier of the edge.",
            unique=True,
            nullable=False,
        )
    ]


class RoadEdgesPenaltiesFile(MetroDataFrameFile):
    path = "calibration/road/edges_penalties.parquet"
    description = "Free-flow time penalties of each road-network edge."
    schema = [
        Column(
            "edge_id",
            MetroDataType.ID,
            description="Identifier of the edge.",
            unique=True,
            nullable=False,
        ),
        Column(
            "constant",
            MetroDataType.FLOAT,
            description="Constant time penalty of the edge, in seconds.",
            nullable=True,
        ),
        Column(
            "speed_multiplier",
            MetroDataType.FLOAT,
            description="By how much edge speed limit is multiplied to get edge free-flow speed.",
            nullable=True,
        ),
    ]


class RoadEdgesFreeFlowTravelTimeFile(MetroDataFrameFile):
    path = "calibration/road/edges_free_flow_travel_time.parquet"
    description = "Free-flow travel time of each road-network edge."
    schema = [
        Column(
            "edge_id",
            MetroDataType.ID,
            description="Identifier of the edge.",
            unique=True,
            nullable=False,
        ),
        Column(
            "free_flow_travel_time",
            MetroDataType.DURATION,
            description="Free-flow travel time of the edge.",
            nullable=False,
        ),
    ]


class AllRoadFreeFlowTravelTimesFile(MetroDataFrameFile):
    path = "calibration/road/all_free_flow_travel_times.parquet"
    description = "Free-flow travel time for each pair of nodes on the road network."
    schema = [
        Column(
            "origin_id",
            MetroDataType.ID,
            description="Identifier of the origine node.",
            nullable=False,
        ),
        Column(
            "destination_id",
            MetroDataType.ID,
            description="Identifier of the destination node.",
            nullable=False,
        ),
        Column(
            "free_flow_travel_time",
            MetroDataType.DURATION,
            description="Free-flow travel time.",
            nullable=True,
        ),
    ]


class AllRoadDistancesFile(MetroDataFrameFile):
    path = "calibration/road/all_distances.parquet"
    description = "Shortest path distance for each pair of nodes on the road network."
    schema = [
        Column(
            "origin_id",
            MetroDataType.ID,
            description="Identifier of the origine node.",
            nullable=False,
        ),
        Column(
            "destination_id",
            MetroDataType.ID,
            description="Identifier of the destination node.",
            nullable=False,
        ),
        Column(
            "distance",
            MetroDataType.FLOAT,
            description="Distance of the shortest path, in meters.",
            nullable=True,
        ),
    ]


class TomTomRoutesFile(MetroGeoDataFrameFile):
    path = "calibration/road/tomtom_routes.geo.parquet"
    description = "Results of the routing requests from TomTom API."
    schema = [
        Column(
            "tomtom_id",
            MetroDataType.ID,
            description="Identifier of the request.",
            unique=True,
            nullable=False,
        ),
        Column(
            "source",
            MetroDataType.ID,
            description="Identifier of the request first node.",
            nullable=False,
        ),
        Column(
            "target",
            MetroDataType.ID,
            description="Identifier of the request last node.",
            nullable=False,
        ),
        Column(
            "length",
            MetroDataType.FLOAT,
            description="Length of the returned path, in meters.",
            nullable=False,
        ),
        Column(
            "departure_time",
            MetroDataType.DATETIME,
            description="Departure time of the request.",
            nullable=False,
        ),
        Column(
            "tt_no_traffic",
            MetroDataType.DURATION,
            description="Travel time on the returned path, under free-flow conditions.",
            nullable=False,
        ),
        Column(
            "tt_traffic",
            MetroDataType.DURATION,
            description="Travel time on the returned path, under congested conditions.",
            nullable=False,
        ),
    ]


class TomTomRoutesMatchedFile(MetroDataFrameFile):
    path = "calibration/road/tomtom_routes_matched.parquet"
    description = "Results of the routing requests from TomTom API after map matching."
    schema = [
        Column(
            "tomtom_id",
            MetroDataType.ID,
            description="Identifier of the request.",
            unique=True,
            nullable=False,
        ),
        Column(
            "length",
            MetroDataType.FLOAT,
            description="Length of the matched path, in meters.",
            nullable=False,
        ),
        Column(
            "length_tomtom",
            MetroDataType.FLOAT,
            description="Length of the TomTom path, in meters.",
            nullable=False,
        ),
        Column(
            "rel_length_diff",
            MetroDataType.FLOAT,
            description="Relative difference between matched and TomTom length.",
            nullable=False,
        ),
        Column(
            "path",
            MetroDataType.LIST_OF_IDS,
            description="Sequence of road network ids that compose the path.",
            nullable=False,
        ),
    ]
