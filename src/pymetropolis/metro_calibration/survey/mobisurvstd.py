from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import BoolParameter, EnumParameter, PathParameter

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

if TYPE_CHECKING:
    import polars as pl
    from mobisurvstd.classes import SurveyData

OFFSET = 1_000_000


def with_survey_name(households: pl.DataFrame, metadata: dict | None) -> pl.DataFrame:
    """Adds a `survey_name` column identifying the survey the households belong to."""
    import polars as pl

    name = metadata.get("name") if metadata is not None else None
    return households.with_columns(survey_name=pl.lit(name, dtype=pl.String))


def prepare_survey(survey: SurveyData, idx: int) -> dict[str, Any]:
    import polars as pl

    if any(len(getattr(survey, k)) >= OFFSET for k in ("households", "persons", "trips", "legs")):
        # Survey ids are offset by OFFSET to prevent duplicate ids over surveys.
        # If a survey has more than OFFSET observations in a DataFrame, duplicate ids mighgt occur.
        raise MetropyError(f"Surveys with more than {OFFSET} observation are not supported.")

    data = dict()
    data["households"] = with_survey_name(
        survey.households.with_columns(pl.col("household_id") + idx * OFFSET), survey.metadata
    ).drop("original_household_id")
    data["persons"] = survey.persons.with_columns(
        pl.col("household_id") + idx * OFFSET, pl.col("person_id") + idx * OFFSET
    ).drop("original_person_id")
    data["trips"] = survey.trips.with_columns(
        pl.col("household_id") + idx * OFFSET,
        pl.col("person_id") + idx * OFFSET,
        pl.col("trip_id") + idx * OFFSET,
    ).drop("original_trip_id")
    data["legs"] = survey.legs.with_columns(
        pl.col("household_id") + idx * OFFSET,
        pl.col("person_id") + idx * OFFSET,
        pl.col("trip_id") + idx * OFFSET,
        pl.col("leg_id") + idx * OFFSET,
    ).drop("original_leg_id")
    data["cars"] = survey.cars.with_columns(
        pl.col("household_id") + idx * OFFSET, pl.col("car_id") + idx * OFFSET
    ).drop("original_car_id")
    data["motorcycles"] = survey.motorcycles.with_columns(
        pl.col("household_id") + idx * OFFSET, pl.col("motorcycle_id") + idx * OFFSET
    ).drop("original_motorcycle_id")
    # Note. For now, zones are not read when in bulk (all ids would need to be adjusted).
    data["special_locations"] = None
    data["detailed_zones"] = None
    data["draw_zones"] = None
    return data


def concatenate_surveys(surveys: list[SurveyData]) -> SurveyData:
    import polars as pl
    from mobisurvstd.classes import SurveyData

    data = dict()
    survey_data = list()
    for i, survey in enumerate(surveys):
        survey_data.append(prepare_survey(survey, i))
    data["households"] = pl.concat([d["households"] for d in survey_data], how="vertical")
    data["persons"] = pl.concat([d["persons"] for d in survey_data], how="vertical")
    data["trips"] = pl.concat([d["trips"] for d in survey_data], how="vertical")
    data["legs"] = pl.concat([d["legs"] for d in survey_data], how="vertical")
    data["cars"] = pl.concat([d["cars"] for d in survey_data], how="vertical")
    data["motorcycles"] = pl.concat([d["motorcycles"] for d in survey_data], how="vertical")
    data["special_locations"] = None
    data["detailed_zones"] = None
    data["draw_zones"] = None
    data["metadata"] = None
    return SurveyData.from_dict(data)


def filter_survey_method(
    households: pl.DataFrame, others: dict[str, pl.DataFrame], method: str
) -> tuple[pl.DataFrame, dict[str, pl.DataFrame]]:
    """Keeps only the households surveyed with the given method, and the rows of the other
    DataFrames (persons, trips, etc.) belonging to these households.
    """
    import polars as pl

    households = households.filter(pl.col("survey_method") == method)
    if households.is_empty():
        raise MetropyError(f"No household in the survey was surveyed with method `{method}`.")
    household_ids = households["household_id"].to_list()
    others = {k: df.filter(pl.col("household_id").is_in(household_ids)) for k, df in others.items()}
    return households, others


class MobiSurvStdImportStep(Step):
    """Reads and standardizes a travel survey compatible with MobiSurvStd.

    Check the [survey page](https://mobisurvstd.github.io/MobiSurvStd/surveys.html) for a list of
    surveys supported by MobiSurvStd.
    """

    survey_path = PathParameter(
        "survey.mobisurvstd.path",
        description="Path to a directory or zipfile containing a survey supported by MobiSurvStd.",
    )
    bulk = BoolParameter(
        "survey.mobisurvstd.bulk",
        description=(
            "If true, all surveys in the provided directory are read in bulk and concatenated."
        ),
        default=False,
    )
    survey_method = EnumParameter(
        "survey.mobisurvstd.survey_method",
        values=["face_to_face", "phone"],
        description=(
            "If set, only households surveyed with this method are kept (along with their "
            "persons, trips, legs, cars and motorcycles). Sample weights are not rescaled."
        ),
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
        assert self.bulk is not None

        if self.bulk:
            surveys = mobisurvstd.bulk_standardize(self.survey_path)
            if surveys is None:
                raise MetropyError(
                    f"Failed to read surveys with MobiSurvStd: `{self.survey_path}`."
                )
            survey = concatenate_surveys(surveys)
        else:
            survey = mobisurvstd.standardize(self.survey_path)
            if survey is None:
                raise MetropyError(f"Failed to read survey with MobiSurvStd: `{self.survey_path}`.")

        households = survey.households
        if not self.bulk:
            # In bulk, `survey_name` is already set for each survey by `prepare_survey` (the
            # metadata of the concatenated survey is `None`).
            households = with_survey_name(households, survey.metadata)
        others = {
            "persons": survey.persons,
            "trips": survey.trips,
            "legs": survey.legs,
            "cars": survey.cars,
            "motorcycles": survey.motorcycles,
        }
        if self.survey_method is not None:
            households, others = filter_survey_method(households, others, self.survey_method)

        self.output["households"].write(households)
        for name, df in others.items():
            self.output[name].write(df)
        if survey.special_locations is not None:
            self.output["special_locations"].write(survey.special_locations)
        if survey.detailed_zones is not None:
            self.output["detailed_zones"].write(survey.detailed_zones)
        if survey.draw_zones is not None:
            self.output["draw_zones"].write(survey.draw_zones)
