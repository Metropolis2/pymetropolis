from pymetropolis.metro_pipeline.file import MetroDataFrameFile, MetroGeoDataFrameFile


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
    discard_extra_columns = False
