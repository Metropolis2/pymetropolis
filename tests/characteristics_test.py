import json
from pathlib import Path

import geopandas as gpd
import polars as pl
import pytest
import shapely

from pymetropolis.metro_calibration.survey import (
    PopulationCharacteristicsStep,
    SurveyCharacteristicsStep,
)
from pymetropolis.metro_calibration.survey.characteristics import (
    compute_characteristics,
    standardize_survey,
    weighted_quantiles,
)
from pymetropolis.metro_calibration.survey.files import (
    SurveyCharacteristicsFile,
    SurveyedDrawZonesFile,
    SurveyedHouseholdsFile,
    SurveyedPersonsFile,
    SurveyedToursFile,
    SurveyedTripsFile,
)
from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_demand.population.files import (
    ExAnteDepartureTimesFile,
    HouseholdsFile,
    HouseholdsHomesFile,
    PersonsFile,
    PopulationCharacteristicsFile,
    ToursFile,
    TripsFile,
)
from pymetropolis.metro_pipeline import Config, MetroPipeline, PopulationStep, Step
from pymetropolis.metro_pipeline.steps import MAIN_POPULATION_NAME
from pymetropolis.metro_spatial.simulation_area.file import SimulationAreaFile


def survey_households() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "household_id": [1, 2],
            "sample_weight": [10.0, 30.0],
            "nb_cars": pl.Series([0, 2], dtype=pl.UInt8),
            "nb_persons": pl.Series([1, 2], dtype=pl.UInt8),
            "home_insee_density": pl.Series([1, None], dtype=pl.UInt8),
            "home_insee_urban_type": ["suburb", "central_city"],
            "home_insee_aav_type": pl.Series([11, 20], dtype=pl.UInt8),
            "home_aav_category": pl.Series([1, 2], dtype=pl.UInt8),
            "home_lng": [0.5, 5.0],
            "home_lat": [0.5, 5.0],
            "home_draw_zone": [1, None],
        }
    )


def survey_persons() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "household_id": [1, 2, 2],
            "woman": [True, False, True],
            "age": pl.Series([10, 30, 50], dtype=pl.UInt8),
            "education_level": [None, "primary", "tertiary"],
            "professional_occupation": ["student", "worker", "other"],
            "detailed_professional_occupation": ["student", "worker:full_time", "other:retired"],
            "pcs_group_code": pl.Series([None, 3, 7], dtype=pl.UInt8),
            "has_driving_license": [None, "yes", "no"],
            "has_public_transit_subscription": [True, False, False],
            "nb_trips": pl.Series([2, 2, None], dtype=pl.UInt8),
            "sample_weight_all": [10.0, 15.0, 15.0],
            "sample_weight_surveyed": [20.0, 20.0, None],
        }
    )


def survey_trips() -> pl.DataFrame:
    urban_columns = {
        f"{side}_{name}": pl.Series([value] * 4, dtype=dtype)
        for side in ("origin", "destination")
        for name, value, dtype in (
            ("insee_density", 1, pl.UInt8),
            ("insee_urban_type", "suburb", pl.String),
            ("insee_aav_type", 11, pl.UInt8),
            ("aav_category", 1, pl.UInt8),
        )
    }
    return pl.DataFrame(
        {
            "trip_id": [1, 2, 3, 4],
            "person_id": [1, 1, 2, 2],
            "household_id": [1, 1, 2, 2],
            "origin_purpose_group": ["home", "work", "home", "shopping"],
            "destination_purpose_group": ["work", "home", "shopping", "home"],
            "destination_activity_duration": pl.Series([60, None, 30, None], dtype=pl.UInt16),
            "departure_time": pl.Series([480, 600, 700, 800], dtype=pl.UInt16),
            "arrival_time": pl.Series([500, 620, 720, 820], dtype=pl.UInt16),
            "trip_euclidean_distance_km": [1.0, 1.0, 2.0, 2.0],
            **urban_columns,
        }
    )


def survey_tours() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "tour_id": ["1-1", "2-1"],
            "household_id": [1, 2],
            "person_id": [1, 2],
            "nb_trips": pl.Series([2, 2], dtype=pl.UInt8),
            "nb_activities": pl.Series([1, 1], dtype=pl.UInt8),
            "trip_ids": [[1, 2], [3, 4]],
            "survey_name": ["a", "a"],
            "weight": [20.0, 60.0],
            "weighted_distance": [1.0, 1.0],
        }
    )


def sim_trips() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trip_id": ["1", "2", "3"],
            "person_id": ["1", "1", "2"],
            "household_id": ["1", "1", "2"],
            "trip_index": pl.Series([1, 2, 1], dtype=pl.UInt8),
            "tour_id": ["1-1", "1-1", "2-1"],
            "origin_purpose_group": ["home", "work", "home"],
            "destination_purpose_group": ["work", "home", "leisure"],
            "destination_activity_duration": pl.Series([3600.0, None, 1800.0]).cast(
                pl.Duration("ms")
            )
            * 1000,
        }
    )


def test_weighted_quantiles():
    values = pl.Series([4.0, 1.0, 3.0, 2.0])
    equal = pl.Series([1.0, 1.0, 1.0, 1.0])
    assert weighted_quantiles(values, equal, [0.0, 0.25, 0.5, 0.75, 1.0]) == [1, 1, 2, 3, 4]
    heavy = pl.Series([1.0, 1.0, 1.0, 97.0])
    assert weighted_quantiles(values, heavy, [0.0, 0.5, 1.0]) == [1, 2, 4]


def test_weighted_statistics():
    households = pl.DataFrame(
        {
            "weight": [1.0, 3.0, None],
            "nb_cars": [0, 2, 10],
            "home_density": ["1", "2", "1"],
            "home_urban_type": [None, "suburb", "suburb"],
        }
    )
    res = compute_characteristics(households, None, None, None)
    assert set(res) == {"households"}
    res = res["households"]
    assert res["sample_count"] == 3
    assert res["count"] == pytest.approx(4.0)
    assert res["nb_cars"]["total"] == pytest.approx(6.0)
    assert res["nb_cars"]["mean"] == pytest.approx(1.5)
    assert res["nb_cars"]["share_positive"] == pytest.approx(0.75)
    assert res["home_density"]["counts"] == {"1": 1.0, "2": 3.0}
    assert res["home_density"]["shares"] == {"1": 0.25, "2": 0.75}
    # NULL values are left out of the shares.
    assert res["home_urban_type"]["shares"] == {"suburb": 1.0}


def test_unweighted_statistics():
    persons = pl.DataFrame({"age": [10, 20, 30, None], "woman": [True, False, None, True]})
    res = compute_characteristics(None, persons, None, None)["persons"]
    assert "sample_count" not in res
    assert res["count"] == 4.0
    assert res["age"] == {
        "min": 10.0,
        "q1": 10.0,
        "median": 20.0,
        "q3": 30.0,
        "max": 30.0,
        "mean": 20.0,
    }
    assert res["woman"]["share"] == pytest.approx(2 / 3)


def test_null_weights_fall_back_to_unit_weights():
    households = pl.DataFrame(
        {"weight": pl.Series([None, None], dtype=pl.Float64), "nb_cars": [0, 2]}
    )
    res = compute_characteristics(households, None, None, None)["households"]
    assert res["count"] == 2.0
    assert res["nb_cars"]["mean"] == pytest.approx(1.0)


def test_missing_variables_are_left_out():
    persons = pl.DataFrame({"person_id": [1], "age": pl.Series([None], dtype=pl.UInt8)})
    res = compute_characteristics(None, persons, None, None)
    assert res == {"persons": {"count": 1.0}}


def test_activity_durations():
    res = compute_characteristics(None, None, sim_trips(), None)["trips"]
    assert res["activity_durations"] == {
        "work": {"total": pytest.approx(3600.0), "share": pytest.approx(2 / 3)},
        "leisure": {"total": pytest.approx(1800.0), "share": pytest.approx(1 / 3)},
    }
    assert res["destination_purpose_group"]["counts"]["home"] == 1.0


def test_standardize_survey():
    households, persons, trips = standardize_survey(
        survey_households(), survey_persons(), survey_trips()
    )
    # Person 1 has an unknown license status.
    assert households["nb_driving_licenses"].to_list() == [None, 1]
    assert persons["professional_activity"].to_list() == ["under14", "full_time_worker", "retired"]
    assert persons["socioprofessional_class"].to_list() == [None, 3, None]
    res = compute_characteristics(households, persons, trips, survey_tours())
    assert res["households"]["home_density"]["counts"] == {"1": 10.0}
    assert res["persons"]["count"] == pytest.approx(40.0)
    # Person 3 was not surveyed for trips.
    assert res["persons"]["nb_trips"]["mean"] == pytest.approx(2.0)
    assert res["persons"]["has_driving_license"]["share"] == pytest.approx(0.5)
    assert res["trips"]["sample_count"] == 4
    assert res["trips"]["count"] == pytest.approx(80.0)
    assert res["trips"]["od_distance"]["total"] == pytest.approx(120_000.0)
    assert res["trips"]["departure_time"]["min"] == pytest.approx(480 * 60)
    assert res["trips"]["activity_durations"]["work"]["total"] == pytest.approx(60 * 60 * 20)
    assert res["trips"]["origin_density"]["shares"] == {"1": 1.0}
    assert res["tours"]["nb_trips"]["mean"] == pytest.approx(2.0)
    json.dumps(res, sort_keys=True, allow_nan=False)


class DummySurveyStep(Step):
    output_files = {
        "households": SurveyedHouseholdsFile,
        "persons": SurveyedPersonsFile,
        "trips": SurveyedTripsFile,
        "tours": SurveyedToursFile,
        "draw_zones": SurveyedDrawZonesFile,
    }

    def run(self):
        self.output["households"].write(survey_households())
        self.output["persons"].write(survey_persons())
        self.output["trips"].write(survey_trips())
        self.output["tours"].write(survey_tours())
        self.output["draw_zones"].write(
            gpd.GeoDataFrame(
                {"draw_zone_id": [1, 2]},
                geometry=[shapely.box(0, 0, 1, 1), shapely.box(4, 4, 6, 6)],
                crs="EPSG:4326",
            )
        )


class DummySimulationAreaStep(Step):
    output_files = {"area": SimulationAreaFile}

    def run(self):
        self.output["area"].write(
            gpd.GeoDataFrame(geometry=[shapely.box(0, 0, 1, 1)], crs="EPSG:4326")
        )


class DummyPopulationStep(PopulationStep):
    output_files = {
        "households": HouseholdsFile,
        "homes": HouseholdsHomesFile,
        "persons": PersonsFile,
        "trips": TripsFile,
        "tours": ToursFile,
        "departure_times": ExAnteDepartureTimesFile,
    }

    def run(self):
        ids = ["1", "2"]
        self.output["households"].write(pl.DataFrame({"household_id": ids}))
        self.output["homes"].write(
            gpd.GeoDataFrame(
                {"household_id": ids},
                geometry=[shapely.Point(0.5, 0.5), shapely.Point(5.0, 5.0)],
                crs="EPSG:4326",
            )
        )
        self.output["persons"].write(
            pl.DataFrame(
                {
                    "person_id": ids,
                    "household_id": ids,
                    "person_index": pl.Series([1, 1], dtype=pl.UInt8),
                }
            )
        )
        self.output["trips"].write(sim_trips())
        self.output["tours"].write(
            pl.DataFrame(
                {
                    "tour_id": ["1-1", "2-1"],
                    "nb_trips": pl.Series([2, 1], dtype=pl.UInt8),
                    "nb_activities": pl.Series([1, 0], dtype=pl.UInt8),
                    "nb_tours": pl.Series([1, 1], dtype=pl.UInt8),
                }
            )
        )
        self.output["departure_times"].write(
            pl.DataFrame(
                {
                    "trip_id": ["1", "2", "3"],
                    "departure_time": pl.Series([1000.0, 2000.0, 3000.0]).cast(pl.Duration("ms"))
                    * 1000,
                }
            )
        )


STEPS: list[type[Step]] = [
    DummySurveyStep,
    DummySimulationAreaStep,
    DummyPopulationStep,
    SurveyCharacteristicsStep,
    PopulationCharacteristicsStep,
]


def run_pipeline(directory: Path, **extra) -> Config:
    config = Config({"main_directory": str(directory), **extra})
    MetroPipeline(config, STEPS).run()
    return config


def read_survey(config: Config) -> dict:
    return SurveyCharacteristicsFile.from_dir(config.main_directory).read()


def read_population(config: Config) -> dict:
    file = PopulationCharacteristicsFile.for_population(MAIN_POPULATION_NAME)
    return file.from_dir(config.main_directory).read()


def test_steps(tmp_path: Path):
    config = run_pipeline(tmp_path)
    survey = read_survey(config)
    assert survey["households"]["sample_count"] == 2
    assert survey["tours"]["count"] == pytest.approx(80.0)
    population = read_population(config)
    assert population["households"] == {"count": 2.0}
    assert population["trips"]["count"] == 3.0
    assert population["trips"]["departure_time"]["max"] == pytest.approx(3000.0)
    assert population["tours"]["nb_trips"]["mean"] == pytest.approx(1.5)


def test_restrict_survey_to_simulation_area(tmp_path: Path):
    config = run_pipeline(
        tmp_path, survey={"characteristics": {"restrict_to_simulation_area": True}}
    )
    survey = read_survey(config)
    assert survey["households"]["sample_count"] == 1
    assert survey["persons"]["sample_count"] == 1
    assert survey["trips"]["sample_count"] == 2
    assert survey["tours"]["sample_count"] == 1


def test_restrict_population_to_survey_area(tmp_path: Path):
    config = run_pipeline(tmp_path, population_characteristics={"restrict_to_survey_area": True})
    population = read_population(config)
    # Only draw zone 1 has an observed household.
    assert population["households"]["count"] == 1.0
    assert population["persons"]["count"] == 1.0
    assert population["trips"]["count"] == 2.0
    assert population["tours"]["count"] == 1.0


def test_restriction_requires_draw_zones(tmp_path: Path):
    class NoDrawZonesSurveyStep(DummySurveyStep):
        def run(self):
            self.output["households"].write(survey_households())
            self.output["persons"].write(survey_persons())
            self.output["trips"].write(survey_trips())
            self.output["tours"].write(survey_tours())

    config = Config(
        {
            "main_directory": str(tmp_path),
            "population_characteristics": {"restrict_to_survey_area": True},
        }
    )
    steps: list[type[Step]] = [
        NoDrawZonesSurveyStep,
        DummyPopulationStep,
        PopulationCharacteristicsStep,
    ]
    with pytest.raises(MetropyError, match="SurveyedDrawZonesFile"):
        MetroPipeline(config, steps).run()
