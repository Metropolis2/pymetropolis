from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType

SCHEMA_ZONE_OD_FREE_FLOW_TT = [
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
    Column(
        "free_flow_travel_time",
        MetroDataType.DURATION,
        description=(
            "Travel time by car between the two zones' road nodes, under free-flow conditions."
        ),
        nullable=False,
    ),
]

SCHEMA_ZONE_OD_CONGESTED_TT = [
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
    Column(
        "congested_travel_time",
        MetroDataType.DURATION,
        description=(
            "Travel time by car between the two zones' road nodes, under "
            "congested conditions, aggregated (median) over the configured time window."
        ),
        nullable=False,
    ),
    Column(
        "congested_travel_time_min",
        MetroDataType.DURATION,
        description=(
            "Minimum travel time over the congested breakpoints falling within the time window."
        ),
        nullable=False,
    ),
    Column(
        "congested_travel_time_max",
        MetroDataType.DURATION,
        description=(
            "Maximum travel time over the congested breakpoints falling within the time window."
        ),
        nullable=False,
    ),
    Column(
        "congested_travel_time_std",
        MetroDataType.DURATION,
        description=(
            "Standard deviation of the travel time over the congested breakpoints falling "
            "within the time window. Zero when only one breakpoint falls in the window."
        ),
        nullable=False,
    ),
]


class ZoneODLevel1FreeFlowTravelTimesFile(MetroDataFrameFile):
    path = "demand/routing/zone1_od_free_flow_travel_times.parquet"
    description = (
        "Travel time by car under free-flow conditions between each pair of Level-1 zones."
    )
    schema = SCHEMA_ZONE_OD_FREE_FLOW_TT


class ZoneODLevel1CongestedTravelTimesFile(MetroDataFrameFile):
    path = "demand/routing/zone1_od_congested_travel_times.parquet"
    description = (
        "Congested travel time by car between each pair of Level-1 zones, aggregated"
        "over a given time window."
    )
    schema = SCHEMA_ZONE_OD_CONGESTED_TT


class ZoneODLevel2FreeFlowTravelTimesFile(MetroDataFrameFile):
    path = "demand/routing/zone2_od_free_flow_travel_times.parquet"
    description = (
        "Travel time by car under free-flow conditions between each pair of Level-2 zones."
    )
    schema = SCHEMA_ZONE_OD_FREE_FLOW_TT


class ZoneODLevel2CongestedTravelTimesFile(MetroDataFrameFile):
    path = "demand/routing/zone2_od_congested_travel_times.parquet"
    description = (
        "Congested travel time by car between each pair of Level-2 zones, aggregated"
        "over a given time window."
    )
    schema = SCHEMA_ZONE_OD_CONGESTED_TT


class ZoneODLevel3FreeFlowTravelTimesFile(MetroDataFrameFile):
    path = "demand/routing/zone3_od_free_flow_travel_times.parquet"
    description = (
        "Travel time by car under free-flow conditions between each pair of Level-3 zones."
    )
    schema = SCHEMA_ZONE_OD_FREE_FLOW_TT


class ZoneODLevel3CongestedTravelTimesFile(MetroDataFrameFile):
    path = "demand/routing/zone3_od_congested_travel_times.parquet"
    description = (
        "Congested travel time by car between each pair of Level-3 zones, aggregated"
        "over a given time window."
    )
    schema = SCHEMA_ZONE_OD_CONGESTED_TT


class ZoneODLevel4FreeFlowTravelTimesFile(MetroDataFrameFile):
    path = "demand/routing/zone4_od_free_flow_travel_times.parquet"
    description = (
        "Travel time by car under free-flow conditions between each pair of Level-4 zones."
    )
    schema = SCHEMA_ZONE_OD_FREE_FLOW_TT


class ZoneODLevel4CongestedTravelTimesFile(MetroDataFrameFile):
    path = "demand/routing/zone4_od_congested_travel_times.parquet"
    description = (
        "Congested travel time by car between each pair of Level-4 zones, aggregated"
        "over a given time window."
    )
    schema = SCHEMA_ZONE_OD_CONGESTED_TT


class ZoneODLevel5FreeFlowTravelTimesFile(MetroDataFrameFile):
    path = "demand/routing/zone5_od_free_flow_travel_times.parquet"
    description = (
        "Travel time by car under free-flow conditions between each pair of Level-5 zones."
    )
    schema = SCHEMA_ZONE_OD_FREE_FLOW_TT


class ZoneODLevel5CongestedTravelTimesFile(MetroDataFrameFile):
    path = "demand/routing/zone5_od_congested_travel_times.parquet"
    description = (
        "Congested travel time by car between each pair of Level-5 zones, aggregated"
        "over a given time window."
    )
    schema = SCHEMA_ZONE_OD_CONGESTED_TT
