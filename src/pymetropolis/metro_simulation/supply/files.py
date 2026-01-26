from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType


class MetroEdgesFile(MetroDataFrameFile):
    path = "run/input/edges.parquet"
    description = "TODO"
    schema = [
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
            "length",
            MetroDataType.FLOAT,
            description="Length of the edge, in meters",
            nullable=False,
        ),
        Column(
            "speed",
            MetroDataType.FLOAT,
            description="Base speed on the edge (m/s)",
            nullable=False,
        ),
        Column(
            "lanes",
            MetroDataType.FLOAT,
            description="Number of lanes on the edge",
            nullable=False,
        ),
        Column(
            "bottleneck_flow",
            MetroDataType.FLOAT,
            description="Maximum incoming and outgoing flow of vehicles at the edge's entry and exit bottlenecks (PCE/s)",
            nullable=True,
            optional=True,
        ),
        Column(
            "bottleneck_flows",
            MetroDataType.LIST_OF_FLOATS,
            description="Time-dependent bottleneck flows",
            nullable=True,
            optional=True,
        ),
        Column(
            "bottleneck_times",
            MetroDataType.LIST_OF_FLOATS,
            description="Timing of the time-dependent bottleneck flows",
            nullable=True,
            optional=True,
        ),
        Column(
            "constant_travel_time",
            MetroDataType.FLOAT,
            description="Constant travel-time penalty (seconds)",
            nullable=True,
            optional=True,
        ),
        Column(
            "overtaking",
            MetroDataType.BOOL,
            description="Whether vehicles can overtake each other to exit the edge",
            nullable=False,
        ),
        # Column(
        #     "speed_density.type",
        #     MetroDataType.STRING,
        #     description="Type of speed-density function used to compute congested travel time",
        #     nullable=True,
        #     optional=True,
        # ),
        # Column(
        #     "speed_density.capacity",
        #     MetroDataType.FLOAT,
        #     description="Capacity of the bottleneck speed-density function (vehicle m/s)",
        #     nullable=True,
        #     optional=True,
        # ),
        # Column(
        #     "speed_density.min_density",
        #     MetroDataType.FLOAT,
        #     description="Edge density below which free-flow speed is used (for ThreeRegimes speed-density)",
        #     nullable=True,
        #     optional=True,
        # ),
        # Column(
        #     "speed_density.jam_density",
        #     MetroDataType.FLOAT,
        #     description="Edge density above which jam speed is used (for ThreeRegimes speed-density)",
        #     nullable=True,
        #     optional=True,
        # ),
        # Column(
        #     "speed_density.jam_speed",
        #     MetroDataType.FLOAT,
        #     description="Traffic jam speed (m/s, for ThreeRegimes speed-density)",
        #     nullable=True,
        #     optional=True,
        # ),
        # Column(
        #     "speed_density.beta",
        #     MetroDataType.FLOAT,
        #     description="Slope parameter for the ThreeRegimes speed-density function",
        #     nullable=True,
        #     optional=True,
        # ),
    ]


class MetroVehicleTypesFile(MetroDataFrameFile):
    path = "run/input/vehicle_types.parquet"
    description = "TODO"
    schema = [
        Column(
            "vehicle_id",
            MetroDataType.ID,
            description="Identifier of the vehicle type",
            unique=True,
            nullable=False,
        ),
        Column(
            "headway",
            MetroDataType.FLOAT,
            description="Typical length between two vehicles from head to head, in meters",
            nullable=False,
        ),
        Column(
            "pce",
            MetroDataType.FLOAT,
            description="Passenger car equivalent of this vehicle type",
            nullable=False,
        ),
        Column(
            "speed_function.type",
            MetroDataType.STRING,
            description="Type of function used to convert from the base edge speed to the vehicle-specific edge speed",
            nullable=True,
            optional=True,
        ),
        Column(
            "speed_function.upper_bound",
            MetroDataType.FLOAT,
            description="Maximum speed allowed for this vehicle type (m/s)",
            nullable=True,
            optional=True,
        ),
        Column(
            "allowed_edges",
            MetroDataType.LIST_OF_IDS,
            description="Identifiers of the edges that this vehicle type is allowed to take",
            nullable=True,
            optional=True,
        ),
        Column(
            "restricted_edges",
            MetroDataType.LIST_OF_IDS,
            description="Identifiers of the edges that this vehicle type cannot take",
            nullable=True,
            optional=True,
        ),
    ]
