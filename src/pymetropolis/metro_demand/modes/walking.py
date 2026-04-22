import polars as pl

from pymetropolis.metro_demand.population import PersonsFile
from pymetropolis.metro_demand.population.files import TripsDestinationsFile, TripsOriginsFile
from pymetropolis.metro_demand.routing.files import TripsPedestrianNodesFile
from pymetropolis.metro_demand.routing.od_pairs import identify_od_pairs
from pymetropolis.metro_network.pedestrian_network.files import PedestrianEdgesCleanFile
from pymetropolis.metro_pipeline.parameters import ListParameter
from pymetropolis.metro_pipeline.types import String
from pymetropolis.metro_spatial import GeoStep
from pymetropolis.random import FloatDistributionParameter, RandomStep, generate_values

from .files import WalkingPreferencesFile


class WalkingPreferencesStep(RandomStep):
    """Generates the preference parameters of traveling by walk, for each trip, from exogenous
    values.

    The following parameters are generated:

    - constant: penalty of traveling by walk, *per trip*
    - value of time / alpha: penalty per hour spent traveling by walk

    The values can be constant over trips or sampled from a specific distribution.
    """

    constant = FloatDistributionParameter(
        "modes.walking.constant",
        default=0.0,
        description="Constant penalty for each walking trip (€).",
    )
    value_of_time = FloatDistributionParameter(
        "modes.walking.alpha", default=0.0, description="Value of time by walk (€/h)."
    )
    input_files = {"persons": PersonsFile}
    output_files = {"walking_preferences": WalkingPreferencesFile}

    def is_defined(self):
        return self.constant != 0.0 or self.value_of_time != 0.0

    def run(self):
        persons: pl.DataFrame = self.input["persons"].read()
        rng = self.get_rng()
        df = persons.select(
            "person_id",
            walking_cst=generate_values(self.constant, len(persons), rng),
            walking_vot=generate_values(self.value_of_time, len(persons), rng),
        )
        self.output["walking_preferences"].write(df)


class PedestrianODNodesFromCoordinatesStep(GeoStep):
    """Identifies nodes on the pedestrian network to be used as origins and destinations of the
    trips.

    First, this Step finds the nearest edge to the origin / destination coordinates.
    Edges whose type is specified in the
    [`forbidden_types`](parameters.md#pedestrian_networkforbidden_types) parameter are excluded from
    that search.
    Then, the origin / destination node is either the source or target of that nearest edge,
    whichever is closer.
    """

    forbidden_types = ListParameter(
        "pedestrian_network.forbidden_types",
        inner=String(),
        default=[],
        description=(
            "List of pedestrian edges' types that *cannot* be used as origin / destination edge."
        ),
        example='`["trunk", "trunk_link"]`',
    )
    input_files = {
        "edges": PedestrianEdgesCleanFile,
        "origins": TripsOriginsFile,
        "destinations": TripsDestinationsFile,
    }
    output_files = {"ods": TripsPedestrianNodesFile}

    def run(self):
        edges = self.input["edges"].read()
        edges = edges.loc[
            ~edges["edge_type"].isin(self.forbidden_types),
            ["edge_id", "geometry", "source", "target"],
        ]
        origins = self.input["origins"].read()
        destinations = self.input["destinations"].read()
        ods = identify_od_pairs(edges, origins, destinations)
        ods = ods.select(
            pl.all()
            .name.replace("origin_", "origin_pedestrian_")
            .name.replace("destination_", "destination_pedestrian_")
        )
        self.output["ods"].write(ods)
