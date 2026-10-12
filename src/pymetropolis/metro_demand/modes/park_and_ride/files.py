from pymetropolis.metro_pipeline.file import (
    Column,
    MetroDataFrameFile,
    MetroDataType,
    MetroGeoDataFrameFile,
    PopulationFile,
)


class ParkAndRideStopsFile(MetroGeoDataFrameFile, PopulationFile):
    path = "demand/{population}/modes/park_and_ride/transfer_stops.parquet"
    description = "Location of the P+R facility for each tour."
    schema = [
        Column(
            "tour_id",
            MetroDataType.ID,
            description="Identifier of the tour",
            unique=True,
            nullable=False,
        ),
        Column(
            "park_and_ride_stop_id",
            MetroDataType.ID,
            description="Identifier of the public-transit stop where the car is parked.",
            nullable=True,
        ),
    ]


class ParkAndRidePreferencesFile(MetroDataFrameFile, PopulationFile):
    path = "demand/{population}/modes/park_and_ride/preferences.parquet"
    description = "Preferences to travel as park-and-ride, for each tour."
    schema = [
        Column(
            "tour_id",
            MetroDataType.ID,
            description="Identifier of the tour.",
            unique=True,
            nullable=False,
        ),
        Column(
            "park_and_ride_cst",
            MetroDataType.FLOAT,
            description="Penalty for each tour as park-and-ride (€).",
            nullable=True,
        ),
        Column(
            "public_transit_vot",
            MetroDataType.FLOAT,
            description="Value of time for the public transit part (€/h).",
            nullable=True,
        ),
        Column(
            "car_vot",
            MetroDataType.FLOAT,
            description="Value of time for the car part (€/h).",
            nullable=True,
        ),
    ]
