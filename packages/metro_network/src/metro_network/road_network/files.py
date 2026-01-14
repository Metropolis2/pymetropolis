from metro_pipeline.file import (
    Column,
    MetroDataFrameFile,
    MetroDataType,
    MetroGeoDataFrameFile,
)

RAW_EDGES_FILE = MetroGeoDataFrameFile(
    "raw_edges",
    path="network/road_network/edges_raw.geo.parquet",
    description="TODO",
    schema=[
        Column(
            "edge_id",
            MetroDataType.ID,
            description="Identifier of the edge",
            unique=True,
            nullable=False,
        ),
        Column(
            "source",
            MetroDataType.ID,
            description="Identifier of the edge’s first node",
            nullable=False,
        ),
        Column(
            "target",
            MetroDataType.ID,
            description="Identifier of the edge’s last node",
            nullable=False,
        ),
        Column(
            "road_type",
            MetroDataType.ID,
            description="Identifier of the edge's road type",
            nullable=False,
            optional=True,
        ),
        Column(
            "length",
            MetroDataType.FLOAT,
            description="Length of the edge, in meters",
            nullable=False,
        ),
        Column(
            "speed_limit",
            MetroDataType.FLOAT,
            description="Speed limit on the edge, in km/h",
            optional=True,
        ),
        Column(
            "lanes", MetroDataType.FLOAT, description="Number of lanes on the edge", optional=True
        ),
        Column(
            "oneway",
            MetroDataType.BOOL,
            description="Whether the edge is part of a one-way road",
            optional=True,
        ),
        Column("toll", MetroDataType.BOOL, description="Whether the edge has toll", optional=True),
        Column(
            "roundabout",
            MetroDataType.BOOL,
            description="Whether the edge is part of a roundabout",
            optional=True,
        ),
        Column(
            "give_way",
            MetroDataType.BOOL,
            description="Whether the edge end intersection has a give-way sign",
            optional=True,
        ),
        Column(
            "stop",
            MetroDataType.BOOL,
            description="Whether the edge end intersection has a stop sign",
            optional=True,
        ),
        Column(
            "traffic_signals",
            MetroDataType.BOOL,
            description="Whether the edge end intersection has traffic signals",
            optional=True,
        ),
        Column(
            "urban",
            MetroDataType.BOOL,
            description="Whether the edge is within a urban area",
            optional=True,
        ),
        Column("name", MetroDataType.STRING, description="Name of the edge", optional=True),
        Column(
            "original_id",
            MetroDataType.ID,
            description="Identifier of the edge in the original data",
            optional=True,
        ),
    ],
)

CLEAN_EDGES_FILE = MetroGeoDataFrameFile(
    "clean_edges",
    path="network/road_network/edges_clean.geo.parquet",
    description="TODO",
    schema=[
        Column(
            "edge_id",
            MetroDataType.ID,
            description="Identifier of the edge",
            unique=True,
            nullable=False,
        ),
        Column(
            "source",
            MetroDataType.ID,
            description="Identifier of the edge’s first node",
            nullable=False,
        ),
        Column(
            "target",
            MetroDataType.ID,
            description="Identifier of the edge’s last node",
            nullable=False,
        ),
        Column(
            "road_type",
            MetroDataType.ID,
            description="Identifier of the edge's road type",
            nullable=False,
            optional=True,
        ),
        Column(
            "length",
            MetroDataType.FLOAT,
            description="Length of the edge, in meters",
            nullable=False,
        ),
        Column(
            "speed_limit",
            MetroDataType.FLOAT,
            description="Speed limit on the edge, in km/h",
            nullable=False,
        ),
        Column(
            "default_speed_limit",
            MetroDataType.BOOL,
            description="Whether the edge speed limit is set from the default value",
            nullable=False,
        ),
        Column(
            "lanes", MetroDataType.FLOAT, description="Number of lanes on the edge", nullable=False
        ),
        Column(
            "default_lanes",
            MetroDataType.BOOL,
            description="Whether the edge lane number is set from the default value",
            nullable=False,
        ),
        Column(
            "oneway",
            MetroDataType.BOOL,
            description="Whether the edge is part of a one-way road",
            nullable=False,
        ),
        Column("toll", MetroDataType.BOOL, description="Whether the edge has toll", nullable=False),
        Column(
            "roundabout",
            MetroDataType.BOOL,
            description="Whether the edge is part of a roundabout",
            nullable=False,
        ),
        Column(
            "give_way",
            MetroDataType.BOOL,
            description="Whether the edge end intersection has a give-way sign",
            nullable=False,
        ),
        Column(
            "stop",
            MetroDataType.BOOL,
            description="Whether the edge end intersection has a stop sign",
            nullable=False,
        ),
        Column(
            "traffic_signals",
            MetroDataType.BOOL,
            description="Whether the edge end intersection has traffic signals",
            nullable=False,
        ),
        Column(
            "urban",
            MetroDataType.BOOL,
            description="Whether the edge is within a urban area",
            nullable=False,
        ),
        Column(
            "source_in_degree",
            MetroDataType.UINT,
            description="Number of incoming edges for the source node",
            nullable=False,
        ),
        Column(
            "source_out_degree",
            MetroDataType.UINT,
            description="Number of outgoing edges for the source node",
            nullable=False,
        ),
        Column(
            "target_in_degree",
            MetroDataType.UINT,
            description="Number of incoming edges for the target node",
            nullable=False,
        ),
        Column(
            "target_out_degree",
            MetroDataType.UINT,
            description="Number of outgoing edges for the target node",
            nullable=False,
        ),
        Column("name", MetroDataType.STRING, description="Name of the edge", optional=True),
        Column(
            "original_id",
            MetroDataType.ID,
            description="Identifier of the edge in the original data",
            optional=True,
        ),
    ],
)


EDGES_CAPACITIES_FILE = MetroDataFrameFile(
    "edges_capacities",
    path="network/road_network/edges_capacities.parquet",
    description="TODO",
    schema=[
        Column(
            "edge_id",
            MetroDataType.ID,
            description="Identifier of the edge",
            unique=True,
            nullable=False,
        ),
        Column(
            "capacity",
            MetroDataType.FLOAT,
            description="Bottleneck capacity of the edge, in PCE per hour",
            nullable=True,
        ),
        Column(
            "capacities",
            MetroDataType.LIST_OF_FLOATS,
            description="Bottleneck capacity of the edge for different time periods, in PCE per hour",
            nullable=True,
        ),
        Column(
            "times",
            MetroDataType.LIST_OF_TIMES,
            description="Time at which the bottleneck capacity changes on the edge",
            nullable=True,
        ),
    ],
)

EDGES_PENALTIES_FILE = MetroDataFrameFile(
    "edges_penalties",
    path="network/road_network/edges_penalties.parquet",
    description="TODO",
    schema=[
        Column(
            "edge_id",
            MetroDataType.ID,
            description="Identifier of the edge",
            unique=True,
            nullable=False,
        ),
        Column(
            "constant",
            MetroDataType.FLOAT,
            description="Constant time penalty of the edge, in seconds",
            nullable=True,
        ),
    ],
)

ALL_FREE_FLOW_TRAVEL_TIMES_FILE = MetroDataFrameFile(
    "all_free_flow_travel_times",
    path="network/road_network/all_free_flow_travel_times.parquet",
    description="Free-flow travel time from any origin to any destination",
    schema=[
        Column(
            "origin_id",
            MetroDataType.ID,
            description="Identifier of the origine node",
            nullable=False,
        ),
        Column(
            "destination_id",
            MetroDataType.ID,
            description="Identifier of the destination node",
            nullable=False,
        ),
        Column(
            "free_flow_travel_time",
            MetroDataType.DURATION,
            description="Free-flow travel time",
            nullable=True,
        ),
    ],
)

ALL_DISTANCES_FILE = MetroDataFrameFile(
    "all_distances_file",
    path="network/road_network/all_distances.parquet",
    description="Shortest path distance from any origin to any destination",
    schema=[
        Column(
            "origin_id",
            MetroDataType.ID,
            description="Identifier of the origine node",
            nullable=False,
        ),
        Column(
            "destination_id",
            MetroDataType.ID,
            description="Identifier of the destination node",
            nullable=False,
        ),
        Column(
            "distance",
            MetroDataType.FLOAT,
            description="Distance of the shortest path, in meters",
            nullable=True,
        ),
    ],
)
