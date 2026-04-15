import polars as pl

from pymetropolis.metro_demand.population.files import (
    TripsDestinationsFile,
    TripsDistancesFile,
    TripsOriginsFile,
)
from pymetropolis.metro_pipeline import Step


class TripDistancesStep(Step):
    """Computes the Euclidean distances between origin and destination for each trip."""

    input_files = {"origins": TripsOriginsFile, "destinations": TripsDestinationsFile}
    output_files = {"distances": TripsDistancesFile}

    def run(self):
        origins = self.input["origins"].read().sort_values("trip_id")
        destinations = self.input["destinations"].read().sort_values("trip_id")
        origins = origins.sort_values("trip_id")
        destinations = destinations.sort_values("trip_id")
        distances = pl.DataFrame(
            {"trip_id": origins["trip_id"], "od_distance": origins.distance(destinations)}
        )
        self.output["distances"].write(distances)
