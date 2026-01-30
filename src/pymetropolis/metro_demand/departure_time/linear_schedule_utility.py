from datetime import timedelta

import polars as pl

from pymetropolis.metro_common.utils import (
    seconds_since_midnight_to_time_pl,
    time_to_seconds_since_midnight,
)
from pymetropolis.metro_demand.population.files import TripsFile
from pymetropolis.metro_pipeline.parameters import (
    DurationParameter,
    EnumParameter,
    FloatParameter,
    TimeParameter,
)
from pymetropolis.metro_pipeline.steps import RandomStep, Step

from .files import LinearScheduleFile, TstarsFile


class LinearScheduleStep(Step):
    """Generates the preference parameters for schedule-delay utility of each trip, using a
    linear-penalty model (à la Arnott, de Palma, Lindsey), from exogenous values.

    For each trip, the following parameters are created:

    - beta: the penalty for starting the following activity earlier than the desired time
    - gamma: the penalty for starting the following activity earlier than the desired time
    - delta: the length of the desired time window

    The values can be constant over trips or sampled from a specific distribution.

    This Step should be combined with a Step that generate desired activity start times.
    """

    # TODO: allow distributed values

    beta = FloatParameter(
        "departure_time.linear_schedule.beta",
        default=0.0,
        description="Penalty for starting an activity earlier than the desired time (€/h).",
    )
    gamma = FloatParameter(
        "departure_time.linear_schedule.gamma",
        default=0.0,
        description="Penalty for starting an activity later than the desired time (€/h).",
    )
    delta = DurationParameter(
        "departure_time.linear_schedule.delta",
        default=timedelta(0),
        description="Length of the desired time window.",
    )
    input_files = {"trips": TripsFile}
    output_files = {"linear_schedule": LinearScheduleFile}

    def run(self):
        trips: pl.DataFrame = self.input["trips"].read()
        df = trips.select(
            "trip_id",
            beta=pl.lit(self.beta),
            gamma=pl.lit(self.gamma),
            delta=pl.lit(self.delta),
        )
        self.output["linear_schedule"].write(df)


class HomogeneousTstarStep(RandomStep):
    """Generates the desired start time of the activity following each trip, from exogenous values.

    The desired start times can be constant over trips or sampled from a specific distribution.
    It is recommended to only use this Step when each person has one trip only.

    This Step should be combined with a Step that generate schedule-utility parameters.
    """

    tstar = TimeParameter(
        "departure_time.linear_schedule.tstar",
        description="Desired start time of the following activity.",
    )
    std = DurationParameter(
        "departure_time.linear_schedule.tstar_std",
        default=timedelta(0),
        description="Standard-deviation of tstar.",
        note="For a uniform distribution, this is half the interval instead.",
    )
    distr = EnumParameter(
        "departure_time.linear_schedule.tstar_distr",
        values=["Uniform", "Normal", "Gaussian"],
        default="Uniform",
        description="Distribution of tstar.",
    )
    input_files = {"trips": TripsFile}
    output_files = {"tstars": TstarsFile}

    def is_defined(self) -> bool:
        return self.tstar is not None

    def run(self):
        trips = self.input["trips"].read()
        if self.std > timedelta(0):
            rng = self.get_rng()
            tstar_float = time_to_seconds_since_midnight(self.tstar)
            std_float = self.std.total_seconds()
            if self.distr == "Uniform":
                tstars = rng.uniform(
                    tstar_float - std_float, tstar_float + std_float, size=len(trips)
                )
            elif self.distr in ("Normal", "Gaussian"):
                tstars = rng.normal(tstar_float, scale=std_float, size=len(trips))
            df = trips.select("trip_id", tstar=pl.Series(tstars)).with_columns(
                tstar=seconds_since_midnight_to_time_pl("tstar")
            )
        else:
            df = trips.select("trip_id", tstar=pl.lit(self.tstar))
        self.output["tstars"].write(df)
