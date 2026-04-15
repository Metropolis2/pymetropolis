from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType


class OutsideOptionPreferencesFile(MetroDataFrameFile):
    path = "demand/population/modes/outside_option/preferences.parquet"
    description = "Utility of the outside option alternative, for each tour."
    schema = [
        Column(
            "tour_id",
            MetroDataType.ID,
            description="Identifier of the tour.",
            unique=True,
            nullable=False,
        ),
        Column(
            "outside_option_cst",
            MetroDataType.FLOAT,
            description="Utility of the outside option (€).",
            nullable=True,
        ),
    ]


class OutsideOptionTravelTimesFile(MetroDataFrameFile):
    path = "demand/population/modes/outside_option/travel_times.parquet"
    description = "Travel time of the outside option alternative, for each tour."
    schema = [
        Column(
            "tour_id",
            MetroDataType.ID,
            description="Identifier of the tour.",
            unique=True,
            nullable=False,
        ),
        Column(
            "outside_option_travel_time",
            MetroDataType.DURATION,
            description="Duration of the tour for the outside option.",
            nullable=False,
        ),
    ]


class PublicTransitPreferencesFile(MetroDataFrameFile):
    path = "demand/population/modes/public_transit/preferences.parquet"
    description = "Preferences to travel by public transit, for each person."
    schema = [
        Column(
            "person_id",
            MetroDataType.ID,
            description="Identifier of the person.",
            unique=True,
            nullable=False,
        ),
        Column(
            "public_transit_cst",
            MetroDataType.FLOAT,
            description="Penalty for each trip in public transit (€).",
            nullable=True,
        ),
        Column(
            "public_transit_vot",
            MetroDataType.FLOAT,
            description="Value of time in public transit (€/h).",
            nullable=True,
        ),
    ]


class PublicTransitTravelTimesFile(MetroDataFrameFile):
    path = "demand/population/modes/public_transit/travel_times.parquet"
    description = "Travel time of each trip, when traveling by public transit."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "public_transit_travel_time",
            MetroDataType.DURATION,
            description="Duration of the trip by public transit.",
            nullable=False,
        ),
    ]


class WalkingPreferencesFile(MetroDataFrameFile):
    path = "demand/population/modes/walking/preferences.parquet"
    description = "Preferences to travel by walk, for each person."
    schema = [
        Column(
            "person_id",
            MetroDataType.ID,
            description="Identifier of the person.",
            unique=True,
            nullable=False,
        ),
        Column(
            "walking_cst",
            MetroDataType.FLOAT,
            description="Penalty for each walking trip (€).",
            nullable=True,
        ),
        Column(
            "walking_vot",
            MetroDataType.FLOAT,
            description="Value of time by walk (€/h).",
            nullable=True,
        ),
    ]
