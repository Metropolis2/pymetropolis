from pymetropolis.metro_common.io import read_geodataframe
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import PathParameter

from .files import RawEdgesFile


class CustomRoadImportStep(Step):
    """Imports a road network from a geospatial file.

    The file must have the same schema has the `RawEdgesFile`.
    """

    edges_file = PathParameter(
        "custom_road_import.edges_file",
        check_file_exists=True,
        description="Path to the geospatial file containing the edges definition.",
        example='`"data/my_edges.geojson"`',
    )
    output_files = {"raw_edges": RawEdgesFile}

    def is_defined(self) -> bool:
        return self.edges_file is not None

    def run(self):
        edges = read_geodataframe(self.edges_file)
        self.output["raw_edges"].write(edges)
