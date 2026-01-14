from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType

TRIP_ZONES_FILE = MetroDataFrameFile(
    "trip_zones",
    path="demand/population/trip_zones.parquet",
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
            "origin_zone_id",
            MetroDataType.ID,
            description="Identifier of the origin zone",
            nullable=False,
        ),
        Column(
            "destination_zone_id",
            MetroDataType.ID,
            description="Identifier of the destination zone",
            nullable=False,
        ),
    ],
)
