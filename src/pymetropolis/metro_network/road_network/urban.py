import geopandas as gpd
import polars as pl
from shapely.prepared import prep

from pymetropolis.metro_network.road_network.files import RawEdgesFile
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_spatial.urban_areas.file import UrbanAreasFile

from .files import UrbanEdgesFile


def add_urban_tag(edges: gpd.GeoDataFrame, urban_areas: gpd.GeoDataFrame):
    """Creates a DataFrame classifying the edges within urban areas."""
    geom = prep(urban_areas.unary_union)
    urban_flag = [geom.contains(g) for g in edges.geometry]
    df = pl.DataFrame({"edge_id": edges["edge_id"], "urban": urban_flag})
    return df


class UrbanEdgesStep(Step):
    """Identifies edges which are part of urban areas.

    An edge is classified as "urban" if it is fully contained within the urban areas of the
    simulation.
    """

    input_files = {"raw_edges": RawEdgesFile, "urban_areas": UrbanAreasFile}
    output_files = {"urban_edges": UrbanEdgesFile}

    def run(self):
        df = add_urban_tag(
            edges=self.input["raw_edges"].read(), urban_areas=self.input["urban_areas"].read()
        )
        self.output["urban_edges"].write(df)
