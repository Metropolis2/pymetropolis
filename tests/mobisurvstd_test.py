import polars as pl
import pytest

from pymetropolis.metro_calibration.survey.mobisurvstd import filter_survey_method
from pymetropolis.metro_common import MetropyError

METHOD = pl.Enum(["face_to_face", "phone"])


def test_filter_survey_method():
    households = pl.DataFrame(
        {"household_id": [1, 2], "survey_method": ["face_to_face", "phone"]},
        schema_overrides={"survey_method": METHOD},
    )
    persons = pl.DataFrame({"household_id": [1, 1, 2], "person_id": [1, 2, 3]})
    trips = pl.DataFrame({"household_id": [1, 2, 2], "trip_id": [1, 2, 3]})
    households, others = filter_survey_method(
        households, {"persons": persons, "trips": trips}, "phone"
    )
    assert households["household_id"].to_list() == [2]
    assert others["persons"]["person_id"].to_list() == [3]
    assert others["trips"]["trip_id"].to_list() == [2, 3]


def test_filter_survey_method_no_info():
    households = pl.DataFrame(
        {"household_id": [1, 2], "survey_method": [None, None]},
        schema_overrides={"survey_method": METHOD},
    )
    with pytest.raises(MetropyError):
        filter_survey_method(households, {}, "face_to_face")
