from pymetropolis.metro_pipeline.file import Column, MetroDataType, MetroGeoDataFrameFile


class PedestrianEdgesRawFile(MetroGeoDataFrameFile):
    path = "network/pedestrian_network/edges_raw.geo.parquet"
    description = "Characteristics of the pedestrian-network edges, before clean up."
    schema = [
        Column(
            "edge_id",
            MetroDataType.ID,
            description="Identifier of the edge.",
            unique=True,
            nullable=False,
        ),
        Column(
            "source",
            MetroDataType.ID,
            description="Identifier of the edge's first node.",
            nullable=False,
        ),
        Column(
            "target",
            MetroDataType.ID,
            description="Identifier of the edge's last node.",
            nullable=False,
        ),
        Column(
            "road_type",
            MetroDataType.ID,
            description="Identifier of the edge's type.",
            nullable=False,
            optional=True,
        ),
        Column(
            "length",
            MetroDataType.FLOAT,
            description="Length of the edge, in meters.",
            nullable=False,
        ),
        Column("name", MetroDataType.STRING, description="Name of the edge.", optional=True),
        Column(
            "original_id",
            MetroDataType.ID,
            description="Identifier of the edge in the original data.",
            optional=True,
        ),
    ]


class PedestrianEdgesCleanFile(MetroGeoDataFrameFile):
    path = "network/pedestrian_network/edges_clean.geo.parquet"
    description = "Characteristics of the pedestrian-network edges, after clean up."
    schema = [
        Column(
            "edge_id",
            MetroDataType.ID,
            description="Identifier of the edge.",
            unique=True,
            nullable=False,
        ),
        Column(
            "source",
            MetroDataType.ID,
            description="Identifier of the edge's first node.",
            nullable=False,
        ),
        Column(
            "target",
            MetroDataType.ID,
            description="Identifier of the edge's last node.",
            nullable=False,
        ),
        Column(
            "road_type",
            MetroDataType.ID,
            description="Identifier of the edge's type.",
            nullable=False,
            optional=True,
        ),
        Column(
            "length",
            MetroDataType.FLOAT,
            description="Length of the edge, in meters.",
            nullable=False,
        ),
        Column("name", MetroDataType.STRING, description="Name of the edge.", optional=True),
        Column(
            "original_id",
            MetroDataType.ID,
            description="Identifier of the edge in the original data.",
            optional=True,
        ),
    ]
