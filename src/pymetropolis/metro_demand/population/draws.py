import polars as pl

from pymetropolis.metro_pipeline.steps import MetroStep

from .files import TripsFile, UniformDrawsFile


class UniformDrawsStep(MetroStep):
    output_files = {"uniform_draws": UniformDrawsFile}

    def required_files(self):
        return {"trips": TripsFile}

    def run(self):
        trips: pl.DataFrame = self.input["trips"].read()
        rng = self.get_rng()
        tour_ids = trips["tour_id"].unique().sort()
        nb_tours = len(tour_ids)
        mode_u = rng.random(size=nb_tours)
        dt_u = rng.random(size=nb_tours)
        df = pl.DataFrame({"tour_id": tour_ids, "mode_u": mode_u, "departure_time_u": dt_u})
        self.output["uniform_draws"].write(df)
