from pymetropolis.metro_pipeline.file import Column, MetroDataType, MetroGeoDataFrameFile


class RoadODMatrixFile(MetroGeoDataFrameFile):
    path = "demand/population/trips/road/origin_destination_matrix.parquet"
    description = "Origin / destination matrix at the road-network node level."
    schema = [
        Column(
            "origin_node_id",
            MetroDataType.ID,
            description="Identifier of the origin node.",
            nullable=False,
        ),
        Column(
            "destination_node_id",
            MetroDataType.ID,
            description="Identifier of the destination node.",
            nullable=False,
        ),
        Column(
            "size",
            MetroDataType.UINT,
            description="Number of trips for this origin-destination pair.",
            nullable=False,
        ),
    ]
