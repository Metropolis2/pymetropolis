from pymetropolis.metro_pipeline.file import Column, MetroDataType, MetroGeoDataFrameFile


class TomTomRoutesFile(MetroGeoDataFrameFile):
    path = "network/road_network/tomtom_results.geo.parquet"
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
