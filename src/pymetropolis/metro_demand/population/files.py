from pymetropolis.metro_pipeline.file import (
    Column,
    MetroDataFrameFile,
    MetroDataType,
    MetroGeoDataFrameFile,
)


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
            MetroDataType.DATETIME,
            description=(
                "Ex-ante departure time from origin. "
                "This can differ from the simulated departure time."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "arrival_time",
            MetroDataType.DATETIME,
            description=(
                "Ex-ante arrival time at destination. "
                "This can differ from the simulated arrival time."
            ),
            nullable=True,
            optional=True,
        ),
    ]


class TripsOriginsFile(MetroGeoDataFrameFile):
    path = "demand/population/trip_origins.geo.parquet"
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


class TripsZonesFile(MetroDataFrameFile):
    path = "demand/population/trips_zones.parquet"
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
        ),
        Column(
            "origin_zone2",
            MetroDataType.ID,
            description="Identifier of the origin location in the level 2 zone system.",
            nullable=True,
        ),
        Column(
            "origin_zone3",
            MetroDataType.ID,
            description="Identifier of the origin location in the level 3 zone system.",
            nullable=True,
        ),
        Column(
            "origin_zone4",
            MetroDataType.ID,
            description="Identifier of the origin location in the level 4 zone system.",
            nullable=True,
        ),
        Column(
            "origin_zone5",
            MetroDataType.ID,
            description="Identifier of the origin location in the level 5 zone system.",
            nullable=True,
        ),
        Column(
            "destination_zone1",
            MetroDataType.ID,
            description="Identifier of the destination location in the level 1 zone system.",
            nullable=True,
        ),
        Column(
            "destination_zone2",
            MetroDataType.ID,
            description="Identifier of the destination location in the level 2 zone system.",
            nullable=True,
        ),
        Column(
            "destination_zone3",
            MetroDataType.ID,
            description="Identifier of the destination location in the level 3 zone system.",
            nullable=True,
        ),
        Column(
            "destination_zone4",
            MetroDataType.ID,
            description="Identifier of the destination location in the level 4 zone system.",
            nullable=True,
        ),
        Column(
            "destination_zone5",
            MetroDataType.ID,
            description="Identifier of the destination location in the level 5 zone system.",
            nullable=True,
        ),
    ]


class TripsDestinationsFile(MetroGeoDataFrameFile):
    path = "demand/population/trip_destinations.geo.parquet"
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


class TripsDistancesFile(MetroDataFrameFile):
    path = "demand/population/trips_distances.parquet"
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
    path = "demand/population/households.parquet"
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
    path = "demand/population/household_homes.geo.parquet"
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
    path = "demand/population/households_zones.parquet"
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
