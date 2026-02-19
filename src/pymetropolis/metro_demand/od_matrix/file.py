from pymetropolis.metro_pipeline.file import (
    Column,
    MetroDataFrameFile,
    MetroDataType,
    MetroGeoDataFrameFile,
)


class TripZonesFile(MetroDataFrameFile):
    path = "demand/population/trip_zones.parquet"
    description = "Origin / destination zones of each trip."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "origin_zone_id",
            MetroDataType.ID,
            description="Identifier of the origin zone.",
            nullable=False,
        ),
        Column(
            "destination_zone_id",
            MetroDataType.ID,
            description="Identifier of the destination zone.",
            nullable=False,
        ),
    ]


class RoadODMatrixFile(MetroGeoDataFrameFile):
    path = "demand/population/road_origin_destination_matrix.parquet"
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
