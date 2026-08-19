from pymetropolis.metro_pipeline.file import Column, MetroDataType, MetroGeoDataFrameFile


class ZonesLevel1File(MetroGeoDataFrameFile):
    path = "areas/zones/zones1.geo.parquet"
    description = "Level-1 zones in the simulated area."
    schema = [
        Column(
            "zone_id",
            MetroDataType.ID,
            description="Identifier of the zone.",
            unique=True,
            nullable=False,
        ),
        Column("name", MetroDataType.STRING, description="Name of the zone.", optional=True),
        Column(
            "original_id",
            MetroDataType.ID,
            description="Identifier of the zone in the original data.",
            optional=True,
        ),
        Column(
            "within_area",
            MetroDataType.BOOL,
            description="Whether the zone is in the simulation area.",
            optional=True,
        ),
    ]


class ZonesLevel2File(MetroGeoDataFrameFile):
    path = "areas/zones/zones2.geo.parquet"
    description = "Level-2 zones in the simulated area."
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
        Column(
            "within_area",
            MetroDataType.BOOL,
            description="Whether the zone is in the simulation area.",
            optional=True,
        ),
    ]


class ZonesLevel3File(MetroGeoDataFrameFile):
    path = "areas/zones/zones3.geo.parquet"
    description = "Level-3 zones in the simulated area."
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
        Column(
            "within_area",
            MetroDataType.BOOL,
            description="Whether the zone is in the simulation area.",
            optional=True,
        ),
    ]


class ZonesLevel4File(MetroGeoDataFrameFile):
    path = "areas/zones/zones4.geo.parquet"
    description = "Level-4 zones in the simulated area."
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
        Column(
            "within_area",
            MetroDataType.BOOL,
            description="Whether the zone is in the simulation area.",
            optional=True,
        ),
    ]


class ZonesLevel5File(MetroGeoDataFrameFile):
    path = "areas/zones/zones5.geo.parquet"
    description = "Level-5 zones in the simulated area."
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
        Column(
            "within_area",
            MetroDataType.BOOL,
            description="Whether the zone is in the simulation area.",
            optional=True,
        ),
    ]
