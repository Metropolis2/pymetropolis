from pymetropolis.metro_pipeline.file import (
    Column,
    MetroDataFrameFile,
    MetroDataType,
    MetroJsonFile,
    MetroPlotFile,
)


class EdgeCapacityFactorsFile(MetroDataFrameFile):
    path = "calibration/road/capacities/edge_factors.parquet"
    description = (
        "Capacity-calibration factors of the Metropolis-Core edges, in long format (one row per "
        "edge and factor applying to it)."
    )
    schema = [
        Column(
            "edge_id",
            MetroDataType.ID,
            description="Identifier of the Metropolis-Core edge.",
            nullable=False,
        ),
        Column(
            "factor",
            MetroDataType.STRING,
            description=(
                'Name of the factor, as `"{variable}={modality}"` (e.g., `"lanes=(1,inf]"`).'
            ),
            nullable=False,
        ),
    ]


class CongestionTimeComparisonPlotFile(MetroPlotFile):
    path = "calibration/road/capacities/comparison.{format}"
    description = (
        "Scatter plot comparing observed and Metropolis-simulated congested "
        "times of the target routes."
    )


class TargetRoutesCongestionTimesFile(MetroDataFrameFile):
    path = "calibration/road/capacities/target_routes_times.parquet"
    description = "Metropolis-simulated travel time and congested time of each target route."
    schema = [
        Column(
            "route_id",
            MetroDataType.ID,
            description="Identifier of the route.",
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


class CapacityFactorsFile(MetroJsonFile):
    path = "calibration/road/capacities/factors.json"
    description = (
        'Capacity factors as a map from factor name (`"{variable}={modality}"`, '
        'e.g., `"lanes=(1,inf]"`) to capacity multiplier.'
    )
