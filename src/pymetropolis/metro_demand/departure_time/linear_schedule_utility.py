from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.io import read_dataframe
from pymetropolis.metro_demand.population.files import TripsFile
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import EnumParameter, PathParameter
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

if TYPE_CHECKING:
    import polars as pl


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

    def is_defined(self):
        return self.beta != 0.0 or self.gamma != 0.0 or self.delta != timedelta(0.0)

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


class LinearScheduleFromPurposeStep(Step):
    """Generates the preference parameters for schedule-delay utility of each trip, from constant
    values over purposes.

    The following parameters are generated:

    - beta: the penalty for starting the following activity earlier than the desired time
    - gamma: the penalty for starting the following activity earlier than the desired time
    - delta: the length of the desired time window

    The
    [`departure_time.linear_schedule.preferences_file`](parameters.md#departure_timelinear_schedulepreferences_file)
    parameter must point to a Parquet or CSV file with the beta, gamma and/or delta values for each
    purpose.
    The file can have the following columns:

    - `purpose`: purpose for which the given values apply
    - `beta`: early penalty, in euros per hour (default is 0 when omitted)
    - `gamma`: late penalty, in euros per hour (default is 0 when omitted)
    - `delta`: desired-time window length, given in number of seconds or directly as Duration dtype
      (default is 0 when omitted)

    For example, to set the beta and gamma values for work and education purposes:

    ```csv
    purpose,beta,gamma
    work,5,15
    education,4,10
    ```
    """

    # TODO: values as function of value of time? (which mode?)
    pref_file = PathParameter(
        "departure_time.linear_schedule.preferences_file",
        check_file_exists=True,
        description=(
            "Path to a Parquet or CSV file with the beta, gamma and delta values for different "
            "activity purposes."
        ),
        note="Possible columns: `purpose`, `beta`, `gamma`, `delta`.",
    )
    input_files = {"trips": TripsFile}
    output_files = {"linear_schedule": LinearScheduleFile}

    def is_defined(self):
        return self.pref_file is not None

    def run(self):
        import polars as pl

        trips: pl.DataFrame = self.input["trips"].read()
        pref = read_dataframe(self.pref_file)
        # Check that the "purpose" column exists.
        if "purpose" not in pref.columns:
            raise MetropyError(f'File `{self.pref_file}` has no "purpose" column.')
        # Cast purpose column to the expected dtype.
        dtype = trips.schema["destination_purpose_group"]
        pref = pref.with_columns(pl.col("purpose").cast(dtype))
        # Check that at least one preference column is present.
        pref_cols = (("beta", pl.Float64), ("gamma", pl.Float64), ("delta", pl.Duration))
        avail_cols = set()
        for col, dtype in pref_cols:
            if col in pref.columns:
                avail_cols.add(col)
                if dtype == pl.Duration:
                    if pref.schema[col] != pl.Duration:
                        # When "delta" column is not given directly as Duration, assume it is
                        # seconds.
                        pref = pref.with_columns(pl.duration(seconds=pl.col(col)))
                else:
                    pref = pref.with_columns(pl.col(col).cast(dtype))
            else:
                pref = pref.with_columns(pl.lit(None, dtype=dtype).alias(col))
        # Send a warning for unused columns in the input file.
        unused_columns = set(pref.columns).difference({"beta", "gamma", "delta", "purpose"})
        if unused_columns:
            for col in unused_columns:
                logger.warning(f"Column `{col}` is ignored.")
            pref = pref.drop(list(unused_columns))
        if not avail_cols:
            raise MetropyError(
                f'File `{self.pref_file}` must have at least one column from: "beta", "gamma", '
                '"delta".'
            )
        df = (
            trips.select("trip_id", "destination_purpose_group")
            .join(pref, left_on="destination_purpose_group", right_on="purpose", how="left")
            .drop("destination_purpose_group")
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


class TstarFromArrivalTimeStep(Step):
    """Generates the desired start time of the activity following each trip, from the trip's ex-ante
    arrival time.

    This Step is useful to generate activity desired start times for the synthetic population
    imported from `EqasimImportStep`.

    To enable this Step, you must set the
    [`departure_time.linear_schedule.tstar_type`](parameters.md#departure_timelinear_scheduletstar_type)
    parameter to `"arrival_time"`.
    """

    tstar_type = EnumParameter(
        "departure_time.linear_schedule.tstar_type",
        values=["arrival_time"],
        description="How tstar values are computed.",
    )
    input_files = {"trips": TripsFile}
    output_files = {"tstars": TstarsFile}

    def is_defined(self):
        return self.tstar_type is not None

    def run(self):
        trips = self.input["trips"].read()
        if "arrival_time" not in trips.columns:
            raise MetropyError("Ex-ante arrival times are not defined for trips.")
        df = trips.select("trip_id", tstar="arrival_time")
        self.output["tstars"].write(df)
