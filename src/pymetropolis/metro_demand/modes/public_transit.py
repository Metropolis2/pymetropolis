import polars as pl

from pymetropolis.metro_demand.population import PersonsFile
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import FloatParameter

from .files import (
    CarDriverDistancesFile,
    PublicTransitPreferencesFile,
    PublicTransitTravelTimesFile,
)


class PublicTransitPreferencesStep(Step):
    constant = FloatParameter(
        "modes.public_transit.constant",
        default=0.0,
        description="Constant penalty for each trip in public transit (€).",
    )
    value_of_time = FloatParameter(
        "modes.public_transit.alpha",
        default=0.0,
        description="Value of time in public transit (€/h).",
    )
    output_files = {"public_transit_preferences": PublicTransitPreferencesFile}

    def required_files(self):
        return {"persons": PersonsFile}

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        df = persons.select(
            "person_id",
            public_transit_cst=pl.lit(self.constant),
            public_transit_vot=pl.lit(self.value_of_time),
        )
        self.output["public_transit_preferences"].write(df)


class PublicTransitTravelTimesFromRoadDistancesStep(Step):
    speed = FloatParameter(
        "modes.public_transit.road_network_speed",
        description="Speed of public-transit vehicles on the road network (km/h).",
    )
    output_files = {"public_transit_travel_times": PublicTransitTravelTimesFile}

    def is_defined(self) -> bool:
        return self.speed is not None

    def required_files(self):
        return {"car_driver_distances": CarDriverDistancesFile}

    def run(self):
        df: pl.DataFrame = self.input["car_driver_distances"].read()
        df = df.select(
            "trip_id",
            public_transit_travel_time=pl.duration(seconds=pl.col("distance") / self.speed * 3.6),
        )
        self.output["public_transit_travel_times"].write(df)
