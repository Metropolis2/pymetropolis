from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType

LINEAR_SCHEDULE_FILE = MetroDataFrameFile(
    "linear_schedule_parameters",
    path="demand/population/linear_schedule_parameters.parquet",
    description="TODO",
    schema=[
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip",
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
    ],
)

TSTAR_FILE = MetroDataFrameFile(
    "tstars",
    path="demand/population/tstars.parquet",
    description="TODO",
    schema=[
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip",
            unique=True,
            nullable=False,
        ),
        Column(
            "tstar",
            MetroDataType.TIME,
            description="Desired start time of the activity following the trip.",
            nullable=True,
        ),
    ],
)
