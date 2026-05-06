from pymetropolis.metro_demand.routing.files import TripsRoadNodesFile
from pymetropolis.metro_pipeline.steps import Step

from .files import PersonsFile, TripsFile


class GenericPopulationStep(Step):
    """Generates a population (persons and trips) from a list of car-driver origin-destination
    pairs.

    Each person has a single trip.
    """

    input_files = {"road_ods": TripsRoadNodesFile}
    output_files = {"trips": TripsFile, "persons": PersonsFile}
    priority = 0

    def run(self):
        import polars as pl

        df: pl.DataFrame = self.input["road_ods"].read()
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
            person_index=pl.lit(1, dtype=pl.UInt8),
            has_driving_license=pl.lit(True, dtype=pl.Boolean),
            has_public_transit_subscription=pl.lit(True, dtype=pl.Boolean),
        )
        self.output["trips"].write(trips)
        self.output["persons"].write(persons)
