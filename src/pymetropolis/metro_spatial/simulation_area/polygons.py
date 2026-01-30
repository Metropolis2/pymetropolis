from pymetropolis.metro_common.io import read_geodataframe
from pymetropolis.metro_pipeline.parameters import FloatParameter, PathParameter
from pymetropolis.metro_spatial import GeoStep

from .common import buffer_area, geom_as_gdf
from .file import SimulationAreaFile


class SimulationAreaFromPolygonsStep(GeoStep):
    """Creates the simulation by reading a geospatial file with polygon(s).

    If you already have a set of polygons which jointly form the entire area (e.g., the
    administrative boundaries of all municipalities to be considered), you can simply provide as
    input a geospatial file with those polygons.
    Then, Pymetropolis will read the file and define the polygon of the simulation area as the union
    of all polygons.
    If there is a single polygon (e.g., the administrative boundary of the region to be considered),
    Pymetropolis will simply use it as the simulation area polygon.

    The file can use any GIS format that can be read by geopandas (e.g., Parquet, Shapefile,
    GeoJson).
    It needs to be specified as the `polygon_file` value.

    ```toml
    [simulation_area]
    polygon_file = "path/to/polygon/filename"
    ```
    """

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
