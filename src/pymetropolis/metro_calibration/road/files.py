from pymetropolis.metro_pipeline.file import (
    Column,
    MetroDataFrameFile,
    MetroDataType,
    MetroGeoDataFrameFile,
)


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


class TargetRoutesFile(MetroDataFrameFile):
    path = "calibration/road/target_routes.parquet"
    description = (
        "Target routes on the road network, with their observed travel times (e.g., TomTom routes "
        "after map matching)."
    )
    schema = [
        Column(
            "route_id",
            MetroDataType.ID,
            description="Identifier of the route.",
            unique=True,
            nullable=False,
        ),
        Column(
            "source",
            MetroDataType.ID,
            description="Identifier of the route first node.",
            nullable=False,
        ),
        Column(
            "target",
            MetroDataType.ID,
            description="Identifier of the route last node.",
            nullable=False,
        ),
        Column(
            "departure_time",
            MetroDataType.DATETIME,
            description="Departure time of the route.",
            nullable=False,
        ),
        Column(
            "target_tt",
            MetroDataType.DURATION,
            description="Observed travel time on the route, under congested conditions.",
            nullable=False,
        ),
        Column(
            "target_free_flow_tt",
            MetroDataType.DURATION,
            description="Observed travel time on the route, under free-flow conditions.",
            optional=True,
            nullable=False,
        ),
        Column(
            "length",
            MetroDataType.FLOAT,
            description="Length of the path, in meters.",
            nullable=False,
        ),
        Column(
            "path",
            MetroDataType.LIST_OF_IDS,
            description="Sequence of road network ids that compose the path.",
            nullable=False,
        ),
    ]
