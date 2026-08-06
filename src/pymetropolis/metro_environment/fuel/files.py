from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType


class CarFuelFile(MetroDataFrameFile):
    path = "demand/population/modes/car/fuel_consumption.parquet"
    description = "Fuel consumption of each car trip."
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


class ParkAndRideFuelFile(MetroDataFrameFile):
    path = "demand/population/modes/park_and_ride/fuel_consumption.parquet"
    description = "Fuel consumption of the car part for each park-and-ride trip."
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
