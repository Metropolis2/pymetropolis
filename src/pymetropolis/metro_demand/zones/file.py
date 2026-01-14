from pymetropolis.metro_pipeline.file import Column, MetroDataType, MetroGeoDataFrameFile

ZONES_FILE = MetroGeoDataFrameFile(
    "zones",
    path="demand/zones/zones.geo.parquet",
    description="TODO",
    schema=[
        Column(
            "zone_id",
            MetroDataType.ID,
            description="Identifier of the zone",
            unique=True,
            nullable=False,
        ),
        Column("name", MetroDataType.STRING, description="Name of the zone", optional=True),
        Column(
            "original_id",
            MetroDataType.ID,
            description="Identifier of the zone in the original data",
            optional=True,
        ),
    ],
)
