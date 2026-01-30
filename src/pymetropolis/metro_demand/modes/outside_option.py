import polars as pl
from loguru import logger

from pymetropolis.metro_demand.population import TripsFile
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import FloatParameter

from .files import (
    CarDriverDistancesFile,
    OutsideOptionPreferencesFile,
    OutsideOptionTravelTimesFile,
)


class OutsideOptionPreferencesStep(Step):
    """Generates the preference parameters of the outside option alternative from exogenous values.

    The following parameters are generated:

    - constant: utility of choosing the outside option alternative
    - value of time / alpha: penalty per hour spent traveling for the outside option (this usually
      irrelevant as the outside option does not imply traveling)

    The values can be constant over tours or sampled from a specific distribution.
    """

    constant = FloatParameter(
        "modes.outside_option.constant",
        default=0.0,
        description="Constant utility of the outside option (€).",
    )
    value_of_time = FloatParameter(
        "modes.outside_option.alpha",
        description="Value of time for the outside option (€/h).",
        note="This is usually not relevant as the outside option does not imply traveling.",
    )
    output_files = {"outside_option_preferences": OutsideOptionPreferencesFile}

    def required_files(self):
        return {"trips": TripsFile}

    def optional_files(self):
        return {"outside_option_travel_times": OutsideOptionTravelTimesFile}

    def run(self):
        trips = self.input["trips"].read()
        df = (
            trips.select("tour_id", outside_option_cst=pl.lit(self.constant, dtype=pl.Float64))
            .unique()
            .sort("tour_id")
        )
        if self.input["outside_option_travel_times"].exists():
            tts: pl.DataFrame = self.input["outside_option_travel_times"].read()
            alpha = self.value_of_time
            if alpha is None:
                logger.warning(
                    "Travel times are defined for the outside option but `modes.outside_option.alpha` is not defined."
                )
            else:
                df = (
                    df.join(tts, on="tour_id", how="left")
                    .with_columns(
                        outside_option_cst=pl.col("outside_option_cst")
                        - alpha * pl.col("outside_option_travel_time").dt.total_seconds() / 3600.0
                    )
                    .drop("outside_option_travel_time")
                )
        elif self.value_of_time is not None and self.value_of_time != 0.0:
            logger.warning(
                "`modes.outside_option.alpha` is defined but there is no travel time for the outside options."
            )
        self.output["outside_option_preferences"].write(df)


class OutsideOptionTravelTimesFromRoadDistancesStep(Step):
    """Generates travel times for the outside option alternatives by applying a constant speed to
    the shortest-path distances of the car-driver trips.

    If a tour has multiple trips, the outside option travel time of the tour depends on the sum of
    the shortest-path distances of all trips.
    """

    speed = FloatParameter(
        "modes.outside_option.road_network_speed",
        description="Constant speed on the road network to compute travel time for outside option trips (km/h).",
    )
    output_files = {"outside_option_travel_times": OutsideOptionTravelTimesFile}

    def is_defined(self) -> bool:
        return self.speed is not None

    def required_files(self):
        return {"car_driver_distances": CarDriverDistancesFile, "trips": TripsFile}

    def run(self):
        df: pl.DataFrame = self.input["car_driver_distances"].read()
        df = df.select(
            "trip_id",
            outside_option_travel_time=pl.duration(seconds=pl.col("distance") / self.speed * 3.6),
        )
        trips = self.input["trips"].read()
        df = df.join(trips, on="trip_id", how="left")
        df = df.group_by("tour_id").agg(pl.col("outside_option_travel_time").sum())
        self.output["outside_option_travel_times"].write(df)
