from pymetropolis.metro_demand.population import PURPOSES
from pymetropolis.metro_pipeline.file import (
    Column,
    MetroDataFrameFile,
    MetroDataType,
    MetroGeoDataFrameFile,
    MetroMLEstimatorFile,
    MetroPlotFile,
    MetroTxtFile,
    PopulationFile,
)


class SurveyedHouseholdsFile(MetroDataFrameFile):
    path = "calibration/survey/households.parquet"
    description = (
        "Households in the survey file. "
        "Variables are documented in the MobiSurvStd documentation for "
        "[households](https://mobisurvstd.github.io/MobiSurvStd/format/households.html)."
    )
    discard_extra_columns = False


class SurveyedPersonsFile(MetroDataFrameFile):
    path = "calibration/survey/persons.parquet"
    description = (
        "Persons in the survey file. "
        "Variables are documented in the MobiSurvStd documentation for "
        "[persons](https://mobisurvstd.github.io/MobiSurvStd/format/persons.html)."
    )
    discard_extra_columns = False


class SurveyedTripsFile(MetroDataFrameFile):
    path = "calibration/survey/trips.parquet"
    description = (
        "Trips in the survey file. "
        "Variables are documented in the MobiSurvStd documentation for "
        "[trips](https://mobisurvstd.github.io/MobiSurvStd/format/trips.html)."
    )
    discard_extra_columns = False


class SurveyedLegsFile(MetroDataFrameFile):
    path = "calibration/survey/legs.parquet"
    description = (
        "Legs in the survey file. "
        "Variables are documented in the MobiSurvStd documentation for "
        "[legs](https://mobisurvstd.github.io/MobiSurvStd/format/legs.html)."
    )
    discard_extra_columns = False


class SurveyedCarsFile(MetroDataFrameFile):
    path = "calibration/survey/cars.parquet"
    description = (
        "Cars in the survey file. "
        "Variables are documented in the MobiSurvStd documentation for "
        "[cars](https://mobisurvstd.github.io/MobiSurvStd/format/cars.html)."
    )
    discard_extra_columns = False


class SurveyedMotorcyclesFile(MetroDataFrameFile):
    path = "calibration/survey/motorcycles.parquet"
    description = (
        "Motorcycles in the survey file. "
        "Variables are documented in the MobiSurvStd documentation for "
        "[motorcycles](https://mobisurvstd.github.io/MobiSurvStd/format/motorcycles.html)."
    )
    discard_extra_columns = False


class SurveyedSpecialLocationsFile(MetroGeoDataFrameFile):
    path = "calibration/survey/special_locations.geo.parquet"
    description = (
        "Special locations in the survey file. "
        "Variables are documented in the MobiSurvStd documentation for "
        "[zones](https://mobisurvstd.github.io/MobiSurvStd/format/zones.html)."
    )
    discard_extra_columns = False


class SurveyedDetailedZonesFile(MetroGeoDataFrameFile):
    path = "calibration/survey/detailed_zones.geo.parquet"
    description = (
        "Detailed zones in the survey file. "
        "Variables are documented in the MobiSurvStd documentation for "
        "[zones](https://mobisurvstd.github.io/MobiSurvStd/format/zones.html)."
    )
    discard_extra_columns = False


class SurveyedDrawZonesFile(MetroGeoDataFrameFile):
    path = "calibration/survey/draw_zones.geo.parquet"
    description = (
        "Draw zones in the survey file. "
        "Variables are documented in the MobiSurvStd documentation for "
        "[zones](https://mobisurvstd.github.io/MobiSurvStd/format/zones.html)."
    )
    discard_extra_columns = False


class SurveyedToursFile(MetroDataFrameFile):
    path = "calibration/survey/tours.parquet"
    description = "Home-based tours in the survey file."
    schema = [
        Column("tour_id", MetroDataType.ID, description="Identifier of the tour.", nullable=False),
        Column(
            "survey_name",
            MetroDataType.STRING,
            description="Name of the survey the tour comes from.",
            nullable=True,
        ),
        Column(
            "household_id",
            MetroDataType.ID,
            description="Identifier of the household of the person who did the tour.",
            nullable=False,
        ),
        Column(
            "person_id",
            MetroDataType.ID,
            description="Identifier of the person who did the tour.",
            nullable=False,
        ),
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
            "modes",
            MetroDataType.LIST_OF_STRINGS,
            description="Main mode taken for each trip of the tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_lngs",
            MetroDataType.LIST_OF_FLOATS,
            description="Longitude at origin for each trip of the tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_lats",
            MetroDataType.LIST_OF_FLOATS,
            description="Latitude at origin for each trip of the tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_lngs",
            MetroDataType.LIST_OF_FLOATS,
            description="Longitude at destination for each trip of the tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_lats",
            MetroDataType.LIST_OF_FLOATS,
            description="Latitude at destination for each trip of the tour.",
            nullable=True,
            optional=True,
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
            description="Whether the tour has trips outside the survey perimeter.",
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
            "tour_mode",
            MetroDataType.STRING,
            description="Main mode used for the tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "other_person_ids",
            MetroDataType.LIST_OF_IDS,
            description="List of ids of household members who did the same tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "other_modes",
            MetroDataType.LIST_OF_STRINGS,
            description="Main tour mode of the household members who did the same tour.",
            nullable=True,
            optional=True,
        ),
        Column(
            "joint_tour",
            MetroDataType.BOOL,
            description="Whether the tour was made jointly with other household members.",
            nullable=True,
            optional=True,
        ),
        Column(
            "lowest_density",
            MetroDataType.ENUM,
            description="Lowest urban density category over activity locations.",
            nullable=True,
            optional=True,
        ),
        Column(
            "highest_density",
            MetroDataType.ENUM,
            description="Highest urban density category over activity locations.",
            nullable=True,
            optional=True,
        ),
        Column(
            "lowest_urban_type",
            MetroDataType.ENUM,
            description="Lowest urban type over activity locations.",
            nullable=True,
            optional=True,
        ),
        Column(
            "highest_urban_type",
            MetroDataType.ENUM,
            description="Highest urban type over activity locations.",
            nullable=True,
            optional=True,
        ),
        Column(
            "lowest_functional_area_type",
            MetroDataType.ENUM,
            description="Lowest functional area type over activity locations.",
            nullable=True,
            optional=True,
        ),
        Column(
            "highest_functional_area_type",
            MetroDataType.ENUM,
            description="Highest functional area type over activity locations.",
            nullable=True,
            optional=True,
        ),
        Column(
            "lowest_functional_area_category",
            MetroDataType.ENUM,
            description="Lowest functional area category over activity locations.",
            nullable=True,
            optional=True,
        ),
        Column(
            "highest_functional_area_category",
            MetroDataType.ENUM,
            description="Highest functional area category over activity locations.",
            nullable=True,
            optional=True,
        ),
        # Household-level variables.
        Column(
            "home_density",
            MetroDataType.ENUM,
            description="Urban density category of the household municipality.",
            nullable=True,
            optional=True,
        ),
        Column(
            "home_urban_type",
            MetroDataType.ENUM,
            description="Urban type of the household municipality.",
            nullable=True,
            optional=True,
        ),
        Column(
            "home_functional_area_type",
            MetroDataType.ENUM,
            description="Type of the municipality within its functional area.",
            nullable=True,
            optional=True,
        ),
        Column(
            "home_functional_area_category",
            MetroDataType.ENUM,
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
            optional=True,
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
            "nb_surveyed",
            MetroDataType.UINT,
            description="Number of persons surveyed for trips in the household.",
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
        Column(
            "household_fully_surveyed",
            MetroDataType.BOOL,
            description="Whether all persons in the household were surveyed for trips.",
            nullable=True,
            optional=True,
        ),
        Column(
            "household_surveyed_on_same_day",
            MetroDataType.BOOL,
            description=(
                "Whether all persons surveyed for trips in the household were surveyed for trips "
                "during the same day."
            ),
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
            description="Professional activity of the person (e.g., worker, student, retiree).",
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
            optional=True,
        ),
        Column(
            "weight",
            MetroDataType.FLOAT,
            description="Statistical survey weight of the person / tour.",
            nullable=True,
        ),
        Column(
            "weighted_distance",
            MetroDataType.FLOAT,
            description=(
                "Statistical survey weight of the person / tour, multiplied by the total Euclidean "
                "distance of trips in the tour."
            ),
            nullable=True,
        ),
    ]


class JointTourEstimatorFile(MetroMLEstimatorFile):
    path = "calibration/survey/joint_tour_estimator.joblib"
    description = "ML estimator for the classification of joint tours."


class ModeEstimatorFile(MetroMLEstimatorFile):
    path = "calibration/survey/mode_estimator.joblib"
    description = "ML estimator for the classification of tours' mode."


class ToursModeShareComparisonFile(MetroTxtFile, PopulationFile):
    path = "calibration/survey/{population}/mode_share_comparison.json"
    description = (
        "JSON file comparing, by tour count and by distance, the mode shares observed in the "
        "survey and the ex-ante mode shares predicted for the simulated population."
    )


class ToursModeShareTourCountPlotFile(MetroPlotFile, PopulationFile):
    path = "calibration/survey/{population}/mode_share_tour_count.png"
    description = "Comparison of survey vs. ex-ante mode shares, by tour count."


class ToursModeShareDistancePlotFile(MetroPlotFile, PopulationFile):
    path = "calibration/survey/{population}/mode_share_distance.png"
    description = "Comparison of survey vs. ex-ante mode shares, by tour distance."
