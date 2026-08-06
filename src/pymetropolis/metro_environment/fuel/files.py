from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType


class CarFuelFile(MetroDataFrameFile):
    path = "demand/population/modes/car/fuel_consumption.parquet"
    description = (
        "Fuel consumption of each car trip, based on the length of the fastest free-flow path."
    )
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "fuel_consumption",
            MetroDataType.FLOAT,
            description="Fuel consumption of the trip, in liters.",
            nullable=True,
        ),
        Column(
            "fuel_cost",
            MetroDataType.FLOAT,
            description="Fuel cost of the trip, in €.",
            nullable=True,
        ),
    ]
