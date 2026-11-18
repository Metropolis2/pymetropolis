from pymetropolis.metro_pipeline.file import (
    Column,
    MetroDataFrameFile,
    MetroDataType,
    MetroPlotFile,
)


class FreeFlowRoadEdgesVariablesFile(MetroDataFrameFile):
    path = "calibration/road/free_flow/edges_variables.parquet"
    description = (
        "Edge-level variables available for free-flow calibration. Defined columns depend on the "
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


class FreeFlowTravelTimeComparisonPlotFile(MetroPlotFile):
    path = "calibration/road/free_flow/travel_time_comparison.{format}"
    description = (
        "Scatter plot comparing observed and Metropolis-simulated free-flow travel "
        "times of the target routes."
    )
