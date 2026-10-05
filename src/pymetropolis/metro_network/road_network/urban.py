from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_network.road_network.files import RoadEdgesRawFile
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_spatial.urban_areas.file import UrbanAreasFile

from .files import RoadEdgesUrbanFlagFile

if TYPE_CHECKING:
    import geopandas as gpd
    import polars as pl


def add_urban_tag(edges: gpd.GeoDataFrame, urban_areas: gpd.GeoDataFrame) -> pl.DataFrame:
    """Creates a DataFrame classifying the edges within urban areas."""
    import numpy as np
    import polars as pl
    import shapely

    # Each polygon of the urban areas only needs to be tested against the edges whose bounding box
    # intersects it (much faster than testing each edge against the whole MultiPolygon).
    parts = shapely.get_parts(urban_areas.geometry.values)
    tree = shapely.STRtree(edges.geometry.values)
    _, edge_idx = tree.query(parts, predicate="contains")
    urban_flag = np.zeros(len(edges), dtype=bool)
    urban_flag[edge_idx] = True
    df = pl.DataFrame({"edge_id": edges["edge_id"], "urban": urban_flag})
    return df


class UrbanEdgesStep(Step):
    """Identifies edges which are part of urban areas.

    An edge is classified as "urban" if it is fully contained within the urban areas of the
    simulation.
    """

    input_files = {"raw_edges": RoadEdgesRawFile, "urban_areas": UrbanAreasFile}
    output_files = {"urban_edges": RoadEdgesUrbanFlagFile}
    priority = 0

    def run(self):
        df = add_urban_tag(
            edges=self.input["raw_edges"].read(), urban_areas=self.input["urban_areas"].read()
        )
        self.output["urban_edges"].write(df)
