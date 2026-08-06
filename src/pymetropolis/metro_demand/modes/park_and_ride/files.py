from pymetropolis.metro_pipeline.file import (
    Column,
    MetroDataFrameFile,
    MetroDataType,
    MetroGeoDataFrameFile,
)


class ParkAndRideStopsFile(MetroGeoDataFrameFile):
    # Note PFR: I set this to a GeoDataFrame (i.e. with the stop Point locations) so that transfer
    # stops can be easily visualized.
    path = "demand/population/modes/park_and_ride/transfer_stops.parquet"
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


class ParkAndRidePreferencesFile(MetroDataFrameFile):
    # Note PFR. I think it's better if we put all the preference parameters for P+R in the same file
    # (including PT and car VOT, even if they are the same as for the unimodal modes).
    path = "demand/population/modes/park_and_ride/preferences.parquet"
    description = "Preferences to travel as park-and-ride, for each person."
    schema = [
        Column(
            "person_id",
            MetroDataType.ID,
            description="Identifier of the person.",
            unique=True,
            nullable=False,
        ),
        Column(
            "park_and_ride_cst",
            MetroDataType.FLOAT,
            description="Penalty for each trip as park-and-ride (€).",
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
