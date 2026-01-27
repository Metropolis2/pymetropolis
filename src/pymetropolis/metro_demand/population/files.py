from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType


class TripsFile(MetroDataFrameFile):
    path = "demand/population/trips.parquet"
    description = "Identifiers and order of the trips for each person."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "person_id",
            MetroDataType.ID,
            description="Identifier of the person performing the trip.",
            nullable=False,
        ),
        Column(
            "household_id",
            MetroDataType.ID,
            description="Identifier of the household to which the person performing the trip belongs.",
            nullable=False,
        ),
        Column(
            "trip_index",
            MetroDataType.UINT,
            description="Index of the trip in the trip chain of the person, starting at 1.",
            nullable=True,
        ),
        Column(
            "tour_id",
            MetroDataType.ID,
            description="Identifier of the home-tour this trip is part of.",
            nullable=False,
        ),
    ]


class PersonsFile(MetroDataFrameFile):
    path = "demand/population/persons.parquet"
    description = "Identifiers and characteristics of the simulated persons."
    schema = [
        Column(
            "person_id",
            MetroDataType.ID,
            description="Identifier of the person.",
            unique=True,
            nullable=False,
        ),
        Column(
            "household_id",
            MetroDataType.ID,
            description="Identifier of the household to which the person belongs.",
            nullable=False,
        ),
        Column(
            "age",
            MetroDataType.UINT,
            description="Age of the person.",
            nullable=True,
        ),
        Column(
            "employed",
            MetroDataType.BOOL,
            description="Whether the person is employed.",
            nullable=True,
        ),
        Column(
            "woman",
            MetroDataType.BOOL,
            description="Whether the person is a woman.",
            nullable=True,
        ),
        Column(
            "socioprofessional_class",
            MetroDataType.UINT,
            description="Socioprofessional class of the person.",
            nullable=True,
        ),
        Column(
            "has_driving_license",
            MetroDataType.BOOL,
            description="Whether the person has a driving license.",
            nullable=True,
        ),
        Column(
            "has_pt_subscription",
            MetroDataType.BOOL,
            description="Whether the person has a public-transit subscription.",
            nullable=True,
        ),
    ]


class HouseholdsFile(MetroDataFrameFile):
    path = "demand/population/households.parquet"
    description = "Identifiers and characteristics of the simulated households."
    schema = [
        Column(
            "household_id",
            MetroDataType.ID,
            description="Identifier of the household.",
            nullable=False,
            unique=True,
        ),
        Column(
            "number_of_persons",
            MetroDataType.UINT,
            description="Number of persons in the household.",
            nullable=False,
        ),
        Column(
            "number_of_vehicles",
            MetroDataType.UINT,
            description="Number of vehicles (cars) owned by the household.",
            nullable=True,
        ),
        Column(
            "number_of_bikes",
            MetroDataType.UINT,
            description="Number of bicycles owned by the household.",
            nullable=True,
        ),
        Column(
            "income",
            MetroDataType.FLOAT,
            description="Monthly disposable income of the household.",
            nullable=True,
        ),
    ]


# TODO. Maybe we should consider having the same mode mu for all the tours of a single person?
class UniformDrawsFile(MetroDataFrameFile):
    path = "demand/population/uniform_draws.parquet"
    description = "Draws for the inverse transform sampling of mode choice and departure-time choice, of each tour."
    schema = [
        Column(
            "tour_id",
            MetroDataType.ID,
            description="Identifier of the tour.",
            nullable=False,
            unique=True,
        ),
        Column(
            "mode_u",
            MetroDataType.FLOAT,
            description="Random uniform draw for mode choice.",
            nullable=False,
        ),
        Column(
            "departure_time_u",
            MetroDataType.FLOAT,
            description="Random uniform draw for departure-time choice.",
            nullable=False,
        ),
    ]
