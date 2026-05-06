from typing import TYPE_CHECKING

from pymetropolis.metro_demand.routing.files import TripsRoadNodesFile
from pymetropolis.metro_network.road_network import RoadEdgesCleanFile
from pymetropolis.random import IntDistributionParameter, RandomStep, generate_int_values

from .common import generate_trips_from_od_matrix

if TYPE_CHECKING:
    import geopandas as gpd


class ODMatrixEachStep(RandomStep):
    """Generates car driver origin-destination pairs by generating a fixed number of trips for each
    node pair of the road network.

    Nodes selected as eligible origins are all nodes with at least one outgoing edge.
    Nodes selected as eligible destinations are all nodes with at least one incoming edge.

    If the road network is not strongly connected, there is no guarantee that all the
    origin-destination pairs generated are feasible (i.e., there is a path from origin to
    destination).
    """

    each = IntDistributionParameter(
        "node_od_matrix.each",
        description="Number of trips to generate for each origin-destination pair.",
    )
    input_files = {"clean_edges": RoadEdgesCleanFile}
    output_files = {"road_ods": TripsRoadNodesFile}

    def is_defined(self) -> bool:
        return self.each is not None

    def run(self):
        import numpy as np
        import polars as pl

        edges: gpd.GeoDataFrame = self.input["clean_edges"].read()
        sources = pl.Series(edges["source"]).unique().sort()
        targets = pl.Series(edges["target"]).unique().sort()
        df = pl.DataFrame(
            {
                "origin": np.repeat(sources, len(targets)),
                "destination": np.tile(targets, len(sources)),
            }
        )
        rng = self.get_rng()
        df = df.with_columns(size=generate_int_values(self.each, len(df), rng))
        trips = generate_trips_from_od_matrix(df, self.get_rng())
        self.output["road_ods"].write(trips)
