import datetime
import tempfile
from pathlib import Path

import polars as pl

from pymetropolis.metro_demand.departure_time import HomogeneousExAnteDepartureTimeStep
from pymetropolis.metro_demand.population.files import ExAnteDepartureTimesFile, TripsFile
from pymetropolis.metro_pipeline import Config, MetroPipeline, PopulationStep
from pymetropolis.metro_pipeline.steps import MAIN_POPULATION_NAME


class DummyTripsStep(PopulationStep):
    output_files = {"trips": TripsFile}

    def run(self):
        n = 500
        self.output["trips"].write(
            pl.DataFrame(
                {
                    "trip_id": [f"{i}-1" for i in range(n)],
                    "household_id": [str(i) for i in range(n)],
                    "person_id": [str(i) for i in range(n)],
                    "trip_index": pl.Series([1] * n, dtype=pl.UInt8),
                    "tour_id": [f"{i}-1" for i in range(n)],
                }
            )
        )


def _run(tmp: Path, extra: dict) -> pl.DataFrame:
    config = Config({"main_directory": str(tmp / "out"), "random_seed": 1, **extra})
    MetroPipeline(config, [DummyTripsStep, HomogeneousExAnteDepartureTimeStep]).run()
    file = ExAnteDepartureTimesFile.for_population(MAIN_POPULATION_NAME)
    return file.from_dir(config.main_directory).read()


def test_uniform_ex_ante_departure_times():
    with tempfile.TemporaryDirectory() as tmp_dir:
        df = _run(
            Path(tmp_dir),
            {
                "departure_time": {
                    "ex_ante": {
                        "departure_time": {
                            "mean": datetime.time(7, 30),
                            "std": 1800,
                            "distribution": "uniform",
                        }
                    }
                }
            },
        )
    assert df.columns == ["trip_id", "departure_time"]
    assert len(df) == 500
    seconds = df["departure_time"].dt.total_seconds()
    assert seconds.is_between(7 * 3600, 8 * 3600).all()
    assert df["departure_time"].n_unique() > 1


def test_constant_ex_ante_departure_times():
    with tempfile.TemporaryDirectory() as tmp_dir:
        df = _run(
            Path(tmp_dir), {"departure_time": {"ex_ante": {"departure_time": datetime.time(8, 0)}}}
        )
    assert (df["departure_time"] == datetime.timedelta(hours=8)).all()
