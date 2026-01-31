import polars as pl

from pymetropolis.random import RandomStep

from .files import TripsFile, UniformDrawsFile


class UniformDrawsStep(RandomStep):
    """Draws random numbers for the inverse transform sampling of mode choice and departure time
    choice of each tour.

    The random numbers are drawn from a uniform distribution between 0 and 1.
    """

    input_files = {"trips": TripsFile}
    output_files = {"uniform_draws": UniformDrawsFile}

    def run(self):
        trips: pl.DataFrame = self.input["trips"].read()
        rng = self.get_rng()
        tour_ids = trips["tour_id"].unique().sort()
        nb_tours = len(tour_ids)
        mode_u = rng.random(size=nb_tours)
        dt_u = rng.random(size=nb_tours)
        df = pl.DataFrame({"tour_id": tour_ids, "mode_u": mode_u, "departure_time_u": dt_u})
        self.output["uniform_draws"].write(df)
