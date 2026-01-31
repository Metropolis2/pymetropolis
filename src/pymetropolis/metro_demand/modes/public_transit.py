import polars as pl

from pymetropolis.metro_demand.population import PersonsFile
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import FloatParameter
from pymetropolis.random import FloatDistributionParameter, RandomStep, generate_values

from .files import (
    CarDriverDistancesFile,
    PublicTransitPreferencesFile,
    PublicTransitTravelTimesFile,
)


class PublicTransitPreferencesStep(RandomStep):
    """Generates the preference parameters of traveling by public transit, for each trip, from
    exogenous values.

    The following parameters are generated:

    - constant: penalty of traveling by public transit, *per trip*
    - value of time / alpha: penalty per hour spent traveling by public transit

    The values can be constant over trips or sampled from a specific distribution.
    """

    constant = FloatDistributionParameter(
        "modes.public_transit.constant",
        default=0.0,
        description="Constant penalty for each trip in public transit (€).",
    )
    value_of_time = FloatDistributionParameter(
        "modes.public_transit.alpha",
        default=0.0,
        description="Value of time in public transit (€/h).",
    )
    input_files = {"persons": PersonsFile}
    output_files = {"public_transit_preferences": PublicTransitPreferencesFile}

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        rng = self.get_rng()
        df = persons.select(
            "person_id",
            public_transit_cst=generate_values(self.constant, len(persons), rng),
            public_transit_vot=generate_values(self.value_of_time, len(persons), rng),
        )
        self.output["public_transit_preferences"].write(df)


class PublicTransitTravelTimesFromRoadDistancesStep(Step):
    """Generates travel times for the public-transit trips by applying a constant speed to
    the shortest-path distances of the car-driver trips.
    """

    speed = FloatParameter(
        "modes.public_transit.road_network_speed",
        description="Speed of public-transit vehicles on the road network (km/h).",
    )
    input_files = {"car_driver_distances": CarDriverDistancesFile}
    output_files = {"public_transit_travel_times": PublicTransitTravelTimesFile}

    def is_defined(self) -> bool:
        return self.speed is not None

    def run(self):
        df: pl.DataFrame = self.input["car_driver_distances"].read()
        df = df.select(
            "trip_id",
            public_transit_travel_time=pl.duration(seconds=pl.col("distance") / self.speed * 3.6),
        )
        self.output["public_transit_travel_times"].write(df)
