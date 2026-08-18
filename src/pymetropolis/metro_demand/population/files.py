from pymetropolis.metro_pipeline.file import (
    Column,
    MetroDataFrameFile,
    MetroDataType,
    MetroGeoDataFrameFile,
)

from .common import PURPOSES


class TripsFile(MetroDataFrameFile):
    path = "demand/population/trips/trips.parquet"
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
            description=(
                "Identifier of the household to which the person performing the trip belongs."
            ),
            nullable=False,
        ),
        Column(
            "trip_index",
            MetroDataType.UINT,
            description="Index of the trip in the trip chain of the person, starting at 1.",
            nullable=False,
        ),
        Column(
            "tour_id",
            MetroDataType.ID,
            description="Identifier of the home-tour this trip is part of.",
            nullable=False,
        ),
        Column(
            "origin_purpose_group",
            MetroDataType.STRING,
            description="Purpose of the activity preceding the trip.",
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_purpose_group",
            MetroDataType.STRING,
            description="Purpose of the activity preceding the trip.",
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_activity_duration",
            MetroDataType.DURATION,
            description="Duration of the activity performed at the trip's origin.",
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_activity_duration",
            MetroDataType.DURATION,
            description="Duration of the activity performed at the trip's destination.",
            nullable=True,
            optional=True,
        ),
        Column(
            "departure_time",
            MetroDataType.DURATION,
            description=(
                "Ex-ante departure time from origin. "
                "This can differ from the simulated departure time."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "arrival_time",
            MetroDataType.DURATION,
            description=(
                "Ex-ante arrival time at destination. "
                "This can differ from the simulated arrival time."
            ),
            nullable=True,
            optional=True,
        ),
    ]


class TripsOriginsFile(MetroGeoDataFrameFile):
    path = "demand/population/trips/origins.geo.parquet"
    description = "Origin coordinates of each trip."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        )
    ]


class TripsDestinationsFile(MetroGeoDataFrameFile):
    path = "demand/population/trips/destinations.geo.parquet"
    description = "Destination coordinates of each trip."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        )
    ]


class ActivitiesLocationsFile(MetroGeoDataFrameFile):
    path = "demand/population/schedule/activity_locations.geo.parquet"
    description = "Coordinates of each activity."
    schema = [
        Column(
            "person_id", MetroDataType.ID, description="Identifier of the person.", nullable=False
        ),
        Column(
            "purpose", MetroDataType.STRING, description="Purpose of the activity.", nullable=True
        ),
        Column(
            "preceding_trip_id",
            MetroDataType.ID,
            description="Identifier of the trip before the activity.",
            nullable=True,
        ),
        Column(
            "following_trip_id",
            MetroDataType.ID,
            description="Identifier of the trip after the activity.",
            nullable=True,
        ),
    ]


class TripsZonesFile(MetroDataFrameFile):
    path = "demand/population/trips/zones.parquet"
    description = "Zones of the trips' origins and destinations."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            nullable=False,
            unique=True,
        ),
        Column(
            "origin_zone1",
            MetroDataType.ID,
            description="Identifier of the origin location in the level 1 zone system.",
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_zone2",
            MetroDataType.ID,
            description="Identifier of the origin location in the level 2 zone system.",
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_zone3",
            MetroDataType.ID,
            description="Identifier of the origin location in the level 3 zone system.",
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_zone4",
            MetroDataType.ID,
            description="Identifier of the origin location in the level 4 zone system.",
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_zone5",
            MetroDataType.ID,
            description="Identifier of the origin location in the level 5 zone system.",
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_zone1",
            MetroDataType.ID,
            description="Identifier of the destination location in the level 1 zone system.",
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_zone2",
            MetroDataType.ID,
            description="Identifier of the destination location in the level 2 zone system.",
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_zone3",
            MetroDataType.ID,
            description="Identifier of the destination location in the level 3 zone system.",
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_zone4",
            MetroDataType.ID,
            description="Identifier of the destination location in the level 4 zone system.",
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_zone5",
            MetroDataType.ID,
            description="Identifier of the destination location in the level 5 zone system.",
            nullable=True,
            optional=True,
        ),
    ]


class TripsDistancesFile(MetroDataFrameFile):
    path = "demand/population/trips/distances.parquet"
    description = "Euclidean distance of each trip."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "od_distance",
            MetroDataType.FLOAT,
            description="Distance between origin and destination, in meters.",
            nullable=False,
        ),
    ]


class PersonsFile(MetroDataFrameFile):
    path = "demand/population/persons/persons.parquet"
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
            "person_index",
            MetroDataType.UINT,
            description="Index of the person within the household's persons.",
            nullable=False,
        ),
        Column(
            "reference_person_link",
            MetroDataType.STRING,
            description="Link of the person relative to the reference person of the household.",
            nullable=False,
            optional=True,
        ),
        Column(
            "woman",
            MetroDataType.BOOL,
            description="Whether the person is a woman.",
            nullable=True,
            optional=True,
        ),
        Column(
            "age",
            MetroDataType.UINT,
            description="Age of the person.",
            nullable=True,
            optional=True,
        ),
        Column(
            "detailed_education_level",
            MetroDataType.STRING,
            description="Highest education level reached by the person, in detailed categories.",
            nullable=True,
            optional=True,
        ),
        Column(
            "education_level",
            MetroDataType.STRING,
            description="Highest education level reached by the person.",
            nullable=True,
            optional=True,
        ),
        Column(
            "professional_activity",
            MetroDataType.STRING,
            description="Professional status of the person.",
            nullable=True,
            optional=True,
        ),
        Column(
            "socioprofessional_class",
            MetroDataType.UINT,
            description="Socioprofessional class of the person.",
            nullable=True,
            optional=True,
        ),
        Column(
            "has_driving_license",
            MetroDataType.BOOL,
            description="Whether the person has a driving license.",
            nullable=True,
            optional=True,
        ),
        Column(
            "has_public_transit_subscription",
            MetroDataType.BOOL,
            description="Whether the person has a public-transit subscription.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_trips",
            MetroDataType.UINT,
            description="Number of trips.",
            nullable=False,
            optional=True,
        ),
    ]


class CarsFile(MetroDataFrameFile):
    path = "demand/population/cars.parquet"
    description = "Characteristics of the cars owned by households / persons."
    schema = [
        Column(
            "car_id",
            MetroDataType.ID,
            description="Identifier of the car.",
            nullable=False,
            unique=True,
        ),
        Column(
            "household_id",
            MetroDataType.ID,
            description="Identifier of the household owning the car.",
            nullable=True,
            optional=True,
        ),
        Column(
            "person_id",
            MetroDataType.ID,
            description="Identifier of the person owning the car.",
            nullable=True,
            optional=True,
        ),
        Column(
            "critair",
            MetroDataType.STRING,
            description="Crit'Air vignette of the car.",
            nullable=True,
            optional=True,
        ),
        Column(
            "fuel_type",
            MetroDataType.STRING,
            description="Fuel type that the car is using.",
            nullable=True,
            optional=True,
        ),
        Column(
            "age", MetroDataType.UINT, description="Age of the car.", nullable=True, optional=True
        ),
        Column(
            "euro_standard",
            MetroDataType.UINT,
            description="Category of the car in the European emission standards.",
            nullable=True,
            optional=True,
        ),
    ]


class HouseholdsFile(MetroDataFrameFile):
    path = "demand/population/households/households.parquet"
    description = (
        "Identifiers and characteristics of the simulated households. "
        "The geometry is a Point representing the household's home."
    )
    schema = [
        Column(
            "household_id",
            MetroDataType.ID,
            description="Identifier of the household.",
            nullable=False,
            unique=True,
        ),
        Column(
            "household_type",
            MetroDataType.STRING,
            description="Type of household family structure.",
            nullable=True,
            optional=True,
        ),
        Column(
            "income",
            MetroDataType.FLOAT,
            description="Monthly income of the household.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_cars",
            MetroDataType.UINT,
            description="Number of cars owned by the household.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_motorcycles",
            MetroDataType.UINT,
            description="Number of motorcycles owned by the household.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_bicycles",
            MetroDataType.UINT,
            description="Number of bicycles owned by the household.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_persons",
            MetroDataType.UINT,
            description="Number of persons in the household.",
            nullable=False,
            optional=True,
        ),
        Column(
            "nb_persons_5plus",
            MetroDataType.UINT,
            description="Number of persons in the household whose age is 5 or plus.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_majors",
            MetroDataType.UINT,
            description="Number of persons in the household whose age is 18 or more.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_minors",
            MetroDataType.UINT,
            description="Number of persons in the household whose age is 17 or less.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_driving_licenses",
            MetroDataType.UINT,
            description="Number of persons in the household with a driving license.",
            nullable=True,
            optional=True,
        ),
    ]


class HouseholdsHomesFile(MetroGeoDataFrameFile):
    path = "demand/population/households/homes.geo.parquet"
    description = "Coordinates of the household homes."
    schema = [
        Column(
            "household_id",
            MetroDataType.ID,
            description="Identifier of the household.",
            unique=True,
            nullable=False,
        )
    ]


class HouseholdsZonesFile(MetroDataFrameFile):
    path = "demand/population/households/zones.parquet"
    description = "Zones of the households' home."
    schema = [
        Column(
            "household_id",
            MetroDataType.ID,
            description="Identifier of the household.",
            nullable=False,
            unique=True,
        ),
        Column(
            "home_zone1",
            MetroDataType.ID,
            description="Identifier of the home location in the level 1 zone system.",
            nullable=True,
            optional=True,
        ),
        Column(
            "home_zone2",
            MetroDataType.ID,
            description="Identifier of the home location in the level 2 zone system.",
            nullable=True,
            optional=True,
        ),
        Column(
            "home_zone3",
            MetroDataType.ID,
            description="Identifier of the home location in the level 3 zone system.",
            nullable=True,
            optional=True,
        ),
        Column(
            "home_zone4",
            MetroDataType.ID,
            description="Identifier of the home location in the level 4 zone system.",
            nullable=True,
            optional=True,
        ),
        Column(
            "home_zone5",
            MetroDataType.ID,
            description="Identifier of the home location in the level 5 zone system.",
            nullable=True,
            optional=True,
        ),
    ]


# TODO. Maybe we should consider having the same mode mu for all the tours of a single person?
class UniformDrawsFile(MetroDataFrameFile):
    path = "demand/population/uniform_draws.parquet"
    description = (
        "Draws for the inverse transform sampling of mode choice and departure-time choice, "
        "of each tour."
    )
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


class ToursFile(MetroDataFrameFile):
    path = "demand/population/tours/tours.parquet"
    description = "Variables at the tour-level."
    schema = [
        Column("tour_id", MetroDataType.ID, description="Identifier of the tour.", nullable=False),
        Column(
            "nb_trips",
            MetroDataType.UINT,
            description="Number of trips in the tour.",
            nullable=False,
        ),
        Column(
            "nb_activities",
            MetroDataType.UINT,
            description="Number of activities in the tour.",
            nullable=False,
        ),
        Column(
            "first_purpose",
            MetroDataType.STRING,
            description="Purpose of the first activity in the tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "last_purpose",
            MetroDataType.STRING,
            description="Purpose of the last activity in the tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "purposes",
            MetroDataType.LIST_OF_STRINGS,
            description="Purpose of each activity in the tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "durations",
            MetroDataType.LIST_OF_DURATIONS,
            description="Duration of each activity in the tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "first_departure_time",
            MetroDataType.DURATION,
            description="Departure time from the first origin of the tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "last_arrival_time",
            MetroDataType.DURATION,
            description="Arrival time at the last destination of the tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "first_activity_start",
            MetroDataType.DURATION,
            description="Start time of the first activity of the tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "last_activity_end",
            MetroDataType.DURATION,
            description="End time of the last activity of the tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "travel_times",
            MetroDataType.LIST_OF_DURATIONS,
            description="Duration of each trip of the tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "distances",
            MetroDataType.LIST_OF_FLOATS,
            description="Euclidean distance of each trip of the tour, in meters.",
            nullable=True,
            optional=True,
        ),
        Column(
            "trip_weekday",
            MetroDataType.STRING,
            description="Weekday during which the tour took place.",
            nullable=True,
            optional=True,
        ),
        Column(
            "outside_perimeter",
            MetroDataType.BOOL,
            description="Whether the tour has trips outside the simulation area.",
            nullable=True,
            optional=True,
        ),
        Column(
            "total_tour_duration",
            MetroDataType.DURATION,
            description=(
                "Duration of the tour, from departure from first origin to arrival at last "
                "destination."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "total_activity_duration",
            MetroDataType.DURATION,
            description="Total duration of all activities in the tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "total_travel_time",
            MetroDataType.DURATION,
            description="Total travel time of all trips in the tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "total_distance",
            MetroDataType.FLOAT,
            description="Total Euclidean distance of all trips in the tour, in meters.",
            nullable=True,
            optional=True,
        ),
        *[
            Column(
                f"has_{purpose}_purpose",
                MetroDataType.BOOL,
                description=f"Whether the tour has at least one activity with '{purpose}' purpose.",
                nullable=True,
                optional=True,
            )
            for purpose in PURPOSES
        ],
        Column(
            "lowest_density",
            MetroDataType.UINT,
            description="Lowest urban density category over activity locations.",
            nullable=True,
            optional=True,
        ),
        Column(
            "highest_density",
            MetroDataType.UINT,
            description="Highest urban density category over activity locations.",
            nullable=True,
            optional=True,
        ),
        Column(
            "lowest_urban_type",
            MetroDataType.STRING,
            description="Lowest urban type over activity locations.",
            nullable=True,
            optional=True,
        ),
        Column(
            "highest_urban_type",
            MetroDataType.STRING,
            description="Highest urban type over activity locations.",
            nullable=True,
            optional=True,
        ),
        Column(
            "lowest_functional_area_type",
            MetroDataType.STRING,
            description="Lowest functional area type over activity locations.",
            nullable=True,
            optional=True,
        ),
        Column(
            "highest_functional_area_type",
            MetroDataType.STRING,
            description="Highest functional area type over activity locations.",
            nullable=True,
            optional=True,
        ),
        Column(
            "lowest_functional_area_category",
            MetroDataType.STRING,
            description="Lowest functional area category over activity locations.",
            nullable=True,
            optional=True,
        ),
        Column(
            "highest_functional_area_category",
            MetroDataType.STRING,
            description="Highest functional area category over activity locations.",
            nullable=True,
            optional=True,
        ),
        # Household-level variables.
        Column(
            "home_density",
            MetroDataType.UINT,
            description="Urban density category of the household municipality.",
            nullable=True,
            optional=True,
        ),
        Column(
            "home_urban_type",
            MetroDataType.STRING,
            description="Urban type of the household municipality.",
            nullable=True,
            optional=True,
        ),
        Column(
            "home_functional_area_type",
            MetroDataType.STRING,
            description="Type of the municipality within its functional area.",
            nullable=True,
            optional=True,
        ),
        Column(
            "home_functional_area_category",
            MetroDataType.STRING,
            description="Category of the functional area of the home municipality.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_cars",
            MetroDataType.UINT,
            description="Number of cars owned by the household.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_motorcycles",
            MetroDataType.UINT,
            description="Number of motorcycles owned by the household.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_bicycles",
            MetroDataType.UINT,
            description="Number of bicycles owned by the household.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_persons",
            MetroDataType.UINT,
            description="Number of persons living in the household.",
            nullable=True,
        ),
        Column(
            "nb_majors",
            MetroDataType.UINT,
            description="Number of persons 18 or older living in the household.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_minors",
            MetroDataType.UINT,
            description="Number of persons 17 or younger living in the household.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_women",
            MetroDataType.UINT,
            description="Number of women living in the household.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_major_women",
            MetroDataType.UINT,
            description="Number of women 18 or older living in the household.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_men",
            MetroDataType.UINT,
            description="Number of men living in the household.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_major_men",
            MetroDataType.UINT,
            description="Number of men 18 or older living in the household.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_driving_licenses",
            MetroDataType.UINT,
            description="Number of driving-license holders living in the household.",
            nullable=True,
            optional=True,
        ),
        Column(
            "household_type",
            MetroDataType.STRING,
            description="Type of household structure (single, couple, singleparent, etc.).",
            nullable=True,
            optional=True,
        ),
        Column(
            "minor_ratio",
            MetroDataType.FLOAT,
            description="Share of household members 17 or younger.",
            nullable=True,
            optional=True,
        ),
        Column(
            "car_ratio",
            MetroDataType.FLOAT,
            description="Number of cars per person in the household.",
            nullable=True,
            optional=True,
        ),
        Column(
            "driving_license_ratio",
            MetroDataType.FLOAT,
            description="Number of cars per driving-license holder.",
            nullable=True,
            optional=True,
        ),
        # Person-level variables.
        Column(
            "woman",
            MetroDataType.BOOL,
            description="Whether the person is a woman.",
            nullable=True,
            optional=True,
        ),
        Column(
            "age",
            MetroDataType.UINT,
            description="Age of the person.",
            nullable=True,
            optional=True,
        ),
        Column(
            "oldest_age_diff",
            MetroDataType.UINT,
            description="Age difference to the oldest person in the household.",
            nullable=True,
            optional=True,
        ),
        Column(
            "education_level",
            MetroDataType.STRING,
            description="Education level of the person.",
            nullable=True,
            optional=True,
        ),
        Column(
            "professional_activity",
            MetroDataType.STRING,
            description="Professional status of the person (e.g., worker, student, retiree).",
            nullable=True,
            optional=True,
        ),
        Column(
            "socioprofessional_class",
            MetroDataType.UINT,
            description="Socioprofessional class (job type) of the person.",
            nullable=True,
            optional=True,
        ),
        Column(
            "has_driving_license",
            MetroDataType.BOOL,
            description="Whether the person has a driving license.",
            nullable=True,
            optional=True,
        ),
        Column(
            "has_public_transit_subscription",
            MetroDataType.BOOL,
            description="Whether the person has a public-transit subscription.",
            nullable=True,
            optional=True,
        ),
        Column(
            "nb_tours",
            MetroDataType.UINT,
            description="Number of tours that the person did during the surveyed day.",
            nullable=True,
        ),
    ]


class JointToursFile(MetroDataFrameFile):
    path = "demand/population/tours/joint_tours.parquet"
    description = "Tour-level flag for joint trips."
    schema = [
        Column("tour_id", MetroDataType.ID, description="Identifier of the tour.", nullable=False),
        Column(
            "joint_tour",
            MetroDataType.BOOL,
            description="Whether the tour is done jointly with another household member.",
            nullable=False,
        ),
    ]
