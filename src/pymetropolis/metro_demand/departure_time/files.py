from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType


class LinearScheduleFile(MetroDataFrameFile):
    path = "demand/population/linear_schedule_parameters.parquet"
    description = "Schedule preferences for each trip, for the linear model."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "beta",
            MetroDataType.FLOAT,
            description="Penalty for starting an activity earlier than the desired time (€/h).",
            nullable=True,
        ),
        Column(
            "gamma",
            MetroDataType.FLOAT,
            description="Penalty for starting an activity later than the desired time (€/h).",
            nullable=True,
        ),
        Column(
            "delta",
            MetroDataType.DURATION,
            description="Length of the desired time window.",
            nullable=True,
        ),
    ]


class TstarsFile(MetroDataFrameFile):
    path = "demand/population/tstars.parquet"
    description = "Desired start time for the activity following each trip."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "tstar",
            MetroDataType.TIME,
            description="Desired start time of the following activity.",
            nullable=True,
        ),
    ]
