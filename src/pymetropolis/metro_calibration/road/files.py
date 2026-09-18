from pymetropolis.metro_pipeline.file import (
    Column,
    MetroDataFrameFile,
    MetroDataType,
    MetroGeoDataFrameFile,
    MetroPlotFile,
)


class RoadEdgesVariablesFile(MetroDataFrameFile):
    path = "calibration/road/edges_variables.parquet"
    description = (
        "Edge-level variables available for calibration. Defined columns depend on the "
        "configuration."
    )
    discard_extra_columns = False
    schema = [
        Column(
            "edge_id",
            MetroDataType.ID,
            description="Identifier of the edge.",
            unique=True,
            nullable=False,
        ),
        Column(
            "base_free_flow_tt",
            MetroDataType.FLOAT,
            description="Free-flow travel time on the edge, based on speed limit, in seconds.",
        ),
    ]


class RoadEdgesPenaltyCoefficientsFile(MetroDataFrameFile):
    path = "calibration/road/free_flow/penalty_coefficients.parquet"
    description = "Coefficients for free-flow time penalties of road-network edges."
    schema = [
        Column(
            "type",
            MetroDataType.STRING,
            description='Penalty type: `"additive"` or `"multiplicative"`.',
            nullable=False,
        ),
        Column(
            "variable1",
            MetroDataType.STRING,
            description="Name of the variable to which the coefficient applies.",
            nullable=False,
        ),
        Column(
            "variable2",
            MetroDataType.STRING,
            description=(
                "Name of the second variable to which the coefficient applies, "
                "for interactions only."
            ),
            nullable=True,
        ),
        Column("penalty", MetroDataType.FLOAT, description="Value of the penalty.", nullable=False),
    ]


class AllRoadFreeFlowTravelTimesFile(MetroDataFrameFile):
    path = "calibration/road/free_flow/all_free_flow_travel_times.parquet"
    description = "Free-flow travel time for each pair of nodes on the road network."
    schema = [
        Column(
            "origin_id",
            MetroDataType.ID,
            description="Identifier of the origine node.",
            nullable=False,
        ),
        Column(
            "destination_id",
            MetroDataType.ID,
            description="Identifier of the destination node.",
            nullable=False,
        ),
        Column(
            "free_flow_travel_time",
            MetroDataType.DURATION,
            description="Free-flow travel time.",
            nullable=True,
        ),
    ]


class TomTomRoutesFile(MetroGeoDataFrameFile):
    path = "calibration/road/tomtom_routes.geo.parquet"
    description = "Results of the routing requests from TomTom API."
    schema = [
        Column(
            "tomtom_id",
            MetroDataType.ID,
            description="Identifier of the request.",
            unique=True,
            nullable=False,
        ),
        Column(
            "source",
            MetroDataType.ID,
            description="Identifier of the request first node.",
            nullable=False,
        ),
        Column(
            "target",
            MetroDataType.ID,
            description="Identifier of the request last node.",
            nullable=False,
        ),
        Column(
            "length",
            MetroDataType.FLOAT,
            description="Length of the returned path, in meters.",
            nullable=False,
        ),
        Column(
            "departure_time",
            MetroDataType.DATETIME,
            description="Departure time of the request.",
            nullable=False,
        ),
        Column(
            "tt_no_traffic",
            MetroDataType.DURATION,
            description="Travel time on the returned path, under free-flow conditions.",
            nullable=False,
        ),
        Column(
            "tt_traffic",
            MetroDataType.DURATION,
            description="Travel time on the returned path, under congested conditions.",
            nullable=False,
        ),
    ]


class FreeFlowTravelTimeComparisonPlotFile(MetroPlotFile):
    path = "calibration/road/free_flow/travel_time_comparison.png"
    description = (
        "Scatter plot comparing TomTom-observed and Metropolis-simulated free-flow travel "
        "times at the OD level."
    )


class TomTomRoutesMatchedFile(MetroDataFrameFile):
    path = "calibration/road/tomtom_routes_matched.parquet"
    description = "Results of the routing requests from TomTom API after map matching."
    schema = [
        Column(
            "tomtom_id",
            MetroDataType.ID,
            description="Identifier of the request.",
            unique=True,
            nullable=False,
        ),
        Column(
            "length",
            MetroDataType.FLOAT,
            description="Length of the matched path, in meters.",
            nullable=False,
        ),
        Column(
            "length_tomtom",
            MetroDataType.FLOAT,
            description="Length of the TomTom path, in meters.",
            nullable=False,
        ),
        Column(
            "rel_length_diff",
            MetroDataType.FLOAT,
            description="Relative difference between matched and TomTom length.",
            nullable=False,
        ),
        Column(
            "path",
            MetroDataType.LIST_OF_IDS,
            description="Sequence of road network ids that compose the path.",
            nullable=False,
        ),
    ]


class CongestionTimeComparisonPlotFile(MetroPlotFile):
    path = "calibration/road/congestion_time_comparison.png"
    description = (
        "Scatter plot comparing TomTom-observed and Metropolis-simulated congested "
        "times at the OD level."
    )


class TomTomCongestionTimesFile(MetroDataFrameFile):
    path = "calibration/road/tomtom_congestion_times.parquet"
    description = (
        "Metropolis-simulated travel time and congested time of each map-matched TomTom route."
    )
    schema = [
        Column(
            "tomtom_id",
            MetroDataType.ID,
            description="Identifier of the request.",
            unique=True,
            nullable=False,
        ),
        Column(
            "travel_time",
            MetroDataType.DURATION,
            description="Simulated travel time on the route.",
            nullable=False,
        ),
        Column(
            "congested_time",
            MetroDataType.DURATION,
            description=(
                "Simulated congested time on the route (travel time minus free-flow time)."
            ),
            nullable=False,
        ),
    ]
