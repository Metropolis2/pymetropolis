from pymetropolis.metro_pipeline.file import (
    Column,
    MetroDataFrameFile,
    MetroDataType,
    MetroGeoDataFrameFile,
)


class BicycleEdgesRawFile(MetroGeoDataFrameFile):
    path = "network/bicycle_network/edges_raw.geo.parquet"
    description = "Characteristics of the bicycle-network edges, before clean up."
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
            "edge_type",
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
        Column(
            "speed_limit",
            MetroDataType.FLOAT,
            description="Speed limit on the edge, for cars, in km/h.",
            optional=True,
        ),
        Column(
            "lanes",
            MetroDataType.FLOAT,
            description="Number of lanes on the edge, for cars.",
            optional=True,
        ),
        Column(
            "roundabout",
            MetroDataType.BOOL,
            description="Whether the edge is part of a roundabout.",
            optional=True,
        ),
        Column(
            "give_way",
            MetroDataType.BOOL,
            description="Whether the edge end intersection has a give-way sign.",
            optional=True,
        ),
        Column(
            "stop",
            MetroDataType.BOOL,
            description="Whether the edge end intersection has a stop sign.",
            optional=True,
        ),
        Column(
            "traffic_signals",
            MetroDataType.BOOL,
            description="Whether the edge end intersection has traffic signals.",
            optional=True,
        ),
        Column(
            "has_bump",
            MetroDataType.BOOL,
            description="Whether there is at least one bump on the edge.",
            optional=True,
        ),
        Column("name", MetroDataType.STRING, description="Name of the edge.", optional=True),
        Column(
            "type",
            MetroDataType.STRING,
            description="Type of the edge for bicycles.",
            optional=True,
        ),
        Column(
            "quality", MetroDataType.UINT, description="Road quality of the edge.", optional=True
        ),
        Column(
            "original_id",
            MetroDataType.ID,
            description="Identifier of the edge in the original data.",
            optional=True,
        ),
    ]


class BicycleEdgesCleanFile(MetroGeoDataFrameFile):
    path = "network/bicycle_network/edges_clean.geo.parquet"
    description = "Characteristics of the bicycle-network edges, after clean up."
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
            "edge_type",
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
        Column(
            "speed_limit",
            MetroDataType.FLOAT,
            description="Speed limit on the edge, for cars, in km/h.",
            optional=True,
        ),
        Column(
            "lanes",
            MetroDataType.FLOAT,
            description="Number of lanes on the edge, for cars.",
            optional=True,
        ),
        Column(
            "roundabout",
            MetroDataType.BOOL,
            description="Whether the edge is part of a roundabout.",
            optional=True,
        ),
        Column(
            "give_way",
            MetroDataType.BOOL,
            description="Whether the edge end intersection has a give-way sign.",
            optional=True,
        ),
        Column(
            "stop",
            MetroDataType.BOOL,
            description="Whether the edge end intersection has a stop sign.",
            optional=True,
        ),
        Column(
            "traffic_signals",
            MetroDataType.BOOL,
            description="Whether the edge end intersection has traffic signals.",
            optional=True,
        ),
        Column(
            "has_bump",
            MetroDataType.BOOL,
            description="Whether there is at least one bump on the edge.",
            optional=True,
        ),
        Column("name", MetroDataType.STRING, description="Name of the edge.", optional=True),
        Column(
            "type",
            MetroDataType.STRING,
            description="Type of the edge for bicycles.",
            optional=True,
        ),
        Column(
            "quality", MetroDataType.UINT, description="Road quality of the edge.", optional=True
        ),
        Column(
            "original_id",
            MetroDataType.ID,
            description="Identifier of the edge in the original data.",
            optional=True,
        ),
    ]


class BicycleEdgesCostsFile(MetroDataFrameFile):
    path = "network/bicycle_network/edges_costs.parquet"
    description = "Cost of each bicycle edge."
    schema = [
        Column(
            "edge_id",
            MetroDataType.ID,
            description="Identifier of the edge.",
            unique=True,
            nullable=False,
        ),
        Column("cost", MetroDataType.FLOAT, description="Cost of the edge.", nullable=True),
    ]
