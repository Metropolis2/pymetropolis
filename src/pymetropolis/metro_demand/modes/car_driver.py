import polars as pl

from pymetropolis.metro_demand.population import PersonsFile
from pymetropolis.metro_network.road_network import AllRoadDistancesFile
from pymetropolis.metro_pipeline import Step
from pymetropolis.random import FloatDistributionParameter, RandomStep, generate_values

from .files import CarDriverDistancesFile, CarDriverODsFile, CarDriverPreferencesFile


class CarDriverPreferencesStep(RandomStep):
    """Generates the preference parameters of traveling as a car driver, for each trip, from
    exogenous values.

    The following parameters are generated:

    - constant: penalty of traveling as car driver, *per trip*
    - value of time / alpha: penalty per hour spent traveling as a car driver

    The values can be constant over trips or sampled from a specific distribution.
    """

    constant = FloatDistributionParameter(
        "modes.car_driver.constant",
        default=0.0,
        description="Constant penalty for each trip as a car driver (€).",
    )
    value_of_time = FloatDistributionParameter(
        "modes.car_driver.alpha",
        default=0.0,
        description="Value of time as a car driver (€/h).",
    )
    input_files = {"persons": PersonsFile}
    output_files = {"car_driver_preferences": CarDriverPreferencesFile}

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        rng = self.get_rng()
        df = persons.select(
            "person_id",
            car_driver_cst=generate_values(self.constant, len(persons), rng),
            car_driver_vot=generate_values(self.value_of_time, len(persons), rng),
        )
        self.output["car_driver_preferences"].write(df)


class CarDriverDistancesStep(Step):
    """Generates the distance of the shortest path on the road network for each trip, given the
    origin and destination as a car driver.

    The shortest-path distances are not computed but are read from the file containing the
    shortest-path distances of all node pairs (AllRoadDistancesFile).
    """

    input_files = {"car_driver_ods": CarDriverODsFile, "all_distances": AllRoadDistancesFile}
    output_files = {"car_driver_distances": CarDriverDistancesFile}

    def run(self):
        trips: pl.DataFrame = self.input["car_driver_ods"].read()
        dists: pl.DataFrame = self.input["all_distances"].read()
        trips = trips.join(
            dists,
            left_on=["origin_node_id", "destination_node_id"],
            right_on=["origin_id", "destination_id"],
            how="left",
        )
        trips = trips.select("trip_id", "distance")
        self.output["car_driver_distances"].write(trips)
