from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType


class CarDriverPreferencesFile(MetroDataFrameFile):
    path = "demand/population/modes/car/car_driver_preferences.parquet"
    description = "Preferences to travel as a car driver, for each person."
    schema = [
        Column(
            "person_id",
            MetroDataType.ID,
            description="Identifier of the person",
            unique=True,
            nullable=False,
        ),
        Column(
            "car_driver_cst",
            MetroDataType.FLOAT,
            description="Penalty for each trip as a car driver (€).",
            nullable=True,
        ),
        Column(
            "car_driver_vot",
            MetroDataType.FLOAT,
            description="Value of time as a car driver (€/h).",
            nullable=True,
        ),
    ]


class CarDriverWithPassengersPreferencesFile(MetroDataFrameFile):
    path = "demand/population/modes/car/car_driver_with_passengers_preferences.parquet"
    description = "Preferences to travel as a car driver with passengers, for each person."
    schema = [
        Column(
            "person_id",
            MetroDataType.ID,
            description="Identifier of the person",
            unique=True,
            nullable=False,
        ),
        Column(
            "car_driver_with_passengers_cst",
            MetroDataType.FLOAT,
            description="Penalty for each trip as a car driver with passengers (€).",
            nullable=True,
        ),
        Column(
            "car_driver_with_passengers_vot",
            MetroDataType.FLOAT,
            description="Value of time as a car driver with passengers (€/h).",
            nullable=True,
        ),
    ]


class CarPassengerPreferencesFile(MetroDataFrameFile):
    path = "demand/population/modes/car/car_passenger_preferences.parquet"
    description = "Preferences to travel as a car passenger, for each person."
    schema = [
        Column(
            "person_id",
            MetroDataType.ID,
            description="Identifier of the person",
            unique=True,
            nullable=False,
        ),
        Column(
            "car_passenger_cst",
            MetroDataType.FLOAT,
            description="Penalty for each trip as a car passenger (€).",
            nullable=True,
        ),
        Column(
            "car_passenger_vot",
            MetroDataType.FLOAT,
            description="Value of time as a car passenger (€/h).",
            nullable=True,
        ),
    ]


class CarRidesharingPreferencesFile(MetroDataFrameFile):
    path = "demand/population/modes/car/car_ridesharing_preferences.parquet"
    description = "Preferences to travel by car ridesharing (driver or passenger), for each person."
    schema = [
        Column(
            "person_id",
            MetroDataType.ID,
            description="Identifier of the person",
            unique=True,
            nullable=False,
        ),
        Column(
            "car_ridesharing_cst",
            MetroDataType.FLOAT,
            description="Penalty for each trip by car ridesharing (€).",
            nullable=True,
        ),
        Column(
            "car_ridesharing_vot",
            MetroDataType.FLOAT,
            description="Value of time by car ridesharing (€/h).",
            nullable=True,
        ),
    ]


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
