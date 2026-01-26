import geopandas as gpd
import numpy as np
import polars as pl

from pymetropolis.metro_demand.modes import CarDriverODsFile
from pymetropolis.metro_network.road_network import CleanEdgesFile
from pymetropolis.metro_pipeline.parameters import FloatParameter
from pymetropolis.metro_pipeline.steps import MetroStep

from .common import generate_trips_from_od_matrix


class ODMatrixEachStep(MetroStep):
    each = FloatParameter(
        "node_od_matrix.each",
        description="Number of trips to generate for each origin-destination pair.",
    )
    output_files = {"car_driver_ods": CarDriverODsFile}

    def is_defined(self) -> bool:
        return self.each is not None

    def required_files(self):
        return {"clean_edges": CleanEdgesFile}

    def run(self):
        edges: gpd.GeoDataFrame = self.input["clean_edges"].read()
        sources = pl.Series(edges["source"]).unique().sort()
        targets = pl.Series(edges["target"]).unique().sort()
        each = self.each
        df = pl.DataFrame(
            {
                "origin": np.repeat(sources, len(targets)),
                "destination": np.tile(targets, len(sources)),
                "size": each,
            }
        )
        trips = generate_trips_from_od_matrix(df, self.get_rng())
        self.output["car_driver_ods_file"].write(trips)
