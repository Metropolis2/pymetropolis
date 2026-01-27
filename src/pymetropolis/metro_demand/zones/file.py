from pymetropolis.metro_pipeline.file import Column, MetroDataType, MetroGeoDataFrameFile


class ZonesFile(MetroGeoDataFrameFile):
    path = "demand/zones/zones.geo.parquet"
    description = "Identifiers and characteristics of the zones in the simulated area."
    schema = [
        Column(
            "zone_id",
            MetroDataType.ID,
            description="Identifier of the zone.",
            unique=True,
            nullable=False,
        ),
        Column("name", MetroDataType.STRING, description="Name of the zone", optional=True),
        Column(
            "original_id",
            MetroDataType.ID,
            description="Identifier of the zone in the original data.",
            optional=True,
        ),
    ]
