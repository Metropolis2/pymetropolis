import polars as pl

from pymetropolis.metro_demand.modes.files import CarDriverODsFile
from pymetropolis.metro_pipeline.steps import Step

from .files import HouseholdsFile, PersonsFile, TripsFile


class GenericPopulationStep(Step):
    """Generates a population (households, persons, and trips) from a list of car-driver
    origin-destination pairs.

    Each household is composed of a single person, with a single trip.
    """

    output_files = {"trips": TripsFile, "persons": PersonsFile, "households": HouseholdsFile}

    def required_files(self):
        return {"car_driver_ods": CarDriverODsFile}

    def run(self):
        df: pl.DataFrame = self.input["car_driver_ods"].read()
        trips = df.select(
            "trip_id",
            person_id="trip_id",
            household_id="trip_id",
            trip_index=pl.lit(1, dtype=pl.UInt8),
            tour_id="trip_id",
        )
        persons = trips.select(
            "person_id",
            "household_id",
            age=pl.lit(None, dtype=pl.UInt8),
            employed=pl.lit(None, dtype=pl.Boolean),
            woman=pl.lit(None, dtype=pl.Boolean),
            socioprofessional_class=pl.lit(None, dtype=pl.UInt8),
            has_driving_license=pl.lit(True, dtype=pl.Boolean),
            has_pt_subscription=pl.lit(True, dtype=pl.Boolean),
        )
        households = trips.select(
            "household_id",
            number_of_persons=pl.lit(1, dtype=pl.UInt8),
            number_of_vehicles=pl.lit(1, dtype=pl.UInt8),
            number_of_bikes=pl.lit(1, dtype=pl.UInt8),
            income=pl.lit(None, dtype=pl.Float64),
        )
        self.output["trips"].write(trips)
        self.output["persons"].write(persons)
        self.output["households"].write(households)
