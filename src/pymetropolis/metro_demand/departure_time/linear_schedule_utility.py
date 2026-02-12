from datetime import timedelta

import polars as pl

from pymetropolis.metro_demand.population.files import TripsFile
from pymetropolis.random import (
    DurationDistributionParameter,
    FloatDistributionParameter,
    RandomStep,
    TimeDistributionParameter,
    generate_duration_values,
    generate_time_values,
    generate_values,
)

from .files import LinearScheduleFile, TstarsFile


class LinearScheduleStep(RandomStep):
    """Generates the preference parameters for schedule-delay utility of each trip, using a
    linear-penalty model (à la Arnott, de Palma, Lindsey), from exogenous values.

    For each trip, the following parameters are created:

    - beta: the penalty for starting the following activity earlier than the desired time
    - gamma: the penalty for starting the following activity earlier than the desired time
    - delta: the length of the desired time window

    The values can be constant over trips or sampled from a specific distribution.

    This Step should be combined with a Step that generate desired activity start times.
    """

    beta = FloatDistributionParameter(
        "departure_time.linear_schedule.beta",
        default=0.0,
        description="Penalty for starting an activity earlier than the desired time (€/h).",
    )
    gamma = FloatDistributionParameter(
        "departure_time.linear_schedule.gamma",
        default=0.0,
        description="Penalty for starting an activity later than the desired time (€/h).",
    )
    delta = DurationDistributionParameter(
        "departure_time.linear_schedule.delta",
        default=timedelta(0.0),
        description="Length of the desired time window.",
    )
    input_files = {"trips": TripsFile}
    output_files = {"linear_schedule": LinearScheduleFile}

    def run(self):
        trips: pl.DataFrame = self.input["trips"].read()
        rng = self.get_rng()
        df = trips.select(
            "trip_id",
            beta=generate_values(self.beta, len(trips), rng),
            gamma=generate_values(self.gamma, len(trips), rng),
            delta=generate_duration_values(self.delta, len(trips), rng),
        )
        self.output["linear_schedule"].write(df)


class HomogeneousTstarStep(RandomStep):
    """Generates the desired start time of the activity following each trip, from exogenous values.

    The desired start times can be constant over trips or sampled from a specific distribution.
    It is recommended to only use this Step when each person has one trip only.

    This Step should be combined with a Step that generate schedule-utility parameters.
    """

    tstar = TimeDistributionParameter(
        "departure_time.linear_schedule.tstar",
        description="Desired start time of the following activity.",
    )
    input_files = {"trips": TripsFile}
    output_files = {"tstars": TstarsFile}

    def is_defined(self) -> bool:
        return self.tstar is not None

    def run(self):
        trips = self.input["trips"].read().select("trip_id")
        tstars = generate_time_values(self.tstar, len(trips), self.get_rng())
        df = trips.with_columns(tstar=tstars)
        self.output["tstars"].write(df)
