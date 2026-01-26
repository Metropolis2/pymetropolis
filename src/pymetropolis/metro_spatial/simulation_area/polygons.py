from pymetropolis.metro_common.io import read_geodataframe
from pymetropolis.metro_pipeline.parameters import FloatParameter, PathParameter
from pymetropolis.metro_spatial import GeoMetroStep

from .common import buffer_area, geom_as_gdf
from .file import SimulationAreaFile


class SimulationAreaFromPolygonsStep(GeoMetroStep):
    polygon_file = PathParameter(
        "simulation_area.polygon_file",
        check_file_exists=True,
        description="Path to the geospatial file containing polygon(s) of the simulation area.",
        example='`"data/my_area.geojson"`',
    )
    buffer = FloatParameter(
        "simulation_area.buffer",
        default=0.0,
        description="Distance by which the polygon of the simulation area must be extended or shrinked.",
        note=(
            "The value is expressed in the unit of measure of the CRS (usually meter). Positive values "
            "extend the area, while negative values shrink it."
        ),
    )
    output_files = {"simulation_area": SimulationAreaFile}

    def is_defined(self) -> bool:
        return self.crs is not None and self.polygon_file is not None

    def run(self):
        filename = self.polygon_file
        gdf = read_geodataframe(filename, columns=["geometry"])
        gdf.to_crs(self.crs, inplace=True)
        geom = gdf.union_all()
        if self.buffer != 0.0:
            geom = buffer_area(geom, self.buffer)
        gdf = geom_as_gdf(geom, self.crs)
        self.output["simulation_area"].write(gdf)
