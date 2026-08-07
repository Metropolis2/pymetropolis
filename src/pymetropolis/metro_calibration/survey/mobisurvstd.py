from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import PathParameter

from .files import (
    SurveyedCarsFile,
    SurveyedDetailedZonesFile,
    SurveyedDrawZonesFile,
    SurveyedHouseholdsFile,
    SurveyedLegsFile,
    SurveyedMotorcyclesFile,
    SurveyedPersonsFile,
    SurveyedSpecialLocationsFile,
    SurveyedTripsFile,
)


class MobiSurvStdImportStep(Step):
    """Reads and standardizes a travel survey compatible with MobiSurvStd.

    Check the [survey page](https://mobisurvstd.github.io/MobiSurvStd/surveys.html) for a list of
    surveys supported by MobiSurvStd.
    """

    survey_path = PathParameter(
        "survey.mobisurvstd.path",
        description="Path to a directory or zipfile containing a survey supported by MobiSurvStd.",
    )
    output_files = {
        "households": SurveyedHouseholdsFile,
        "persons": SurveyedPersonsFile,
        "trips": SurveyedTripsFile,
        "legs": SurveyedLegsFile,
        "cars": SurveyedCarsFile,
        "motorcycles": SurveyedMotorcyclesFile,
        "special_locations": SurveyedSpecialLocationsFile,
        "detailed_zones": SurveyedDetailedZonesFile,
        "draw_zones": SurveyedDrawZonesFile,
    }

    def is_defined(self) -> bool:
        return self.survey_path is not None

    def run(self):
        import mobisurvstd

        assert self.survey_path is not None

        survey = mobisurvstd.standardize(self.survey_path)
        if survey is None:
            raise MetropyError(f"Failed to read survey with MobiSurvStd: `{self.survey_path}`.")

        self.output["households"].write(survey.households)
        self.output["persons"].write(survey.persons)
        self.output["trips"].write(survey.trips)
        self.output["legs"].write(survey.legs)
        self.output["cars"].write(survey.cars)
        self.output["motorcycles"].write(survey.motorcycles)
        if survey.special_locations is not None:
            self.output["special_locations"].write(survey.special_locations)
        if survey.detailed_zones is not None:
            self.output["detailed_zones"].write(survey.detailed_zones)
        if survey.draw_zones is not None:
            self.output["draw_zones"].write(survey.draw_zones)
