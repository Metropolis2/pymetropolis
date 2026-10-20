from __future__ import annotations

from pymetropolis.metro_demand.population.files import ExAnteDepartureTimesFile, TripsFile
from pymetropolis.metro_pipeline import PopulationStep
from pymetropolis.random import RandomStep, TimeDistributionParameter, generate_time_values


class HomogeneousExAnteDepartureTimeStep(RandomStep, PopulationStep):
    """Generates the ex-ante departure time of each trip, from exogenous values.

    The departure times can be constant over trips or sampled from a specific distribution.
    For a `"uniform"` distribution, the values are sampled between `mean - std` and `mean + std`.
    For example, departure times uniformly distributed between 7 a.m. and 8 a.m. are obtained with
    `{ mean = 07:30:00, std = 1800, distribution = "uniform" }`.

    The draws are independent across trips so it is recommended to only use this Step when each
    tour has one trip only.
    """

    departure_time = TimeDistributionParameter(
        "departure_time.ex_ante.departure_time",
        description="Ex-ante departure time of the trips.",
        note=(
            "For a uniform distribution, values are sampled between `mean - std` and "
            "`mean + std`, with `std` in seconds."
        ),
    )
    input_files = {"trips": TripsFile}
    output_files = {"departure_times": ExAnteDepartureTimesFile}

    def is_defined(self) -> bool:
        return self.departure_time is not None

    def run(self):
        trips = self.input["trips"].read().select("trip_id")
        values = generate_time_values(self.departure_time, len(trips), self.get_rng(str(self)))
        df = trips.with_columns(departure_time=values)
        self.output["departure_times"].write(df)
