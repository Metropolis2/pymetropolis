from pathlib import Path

import geopandas as gpd
import osmium
import pyproj
from loguru import logger
from osmium.filter import TagFilter
from osmium.geom import WKBFactory

from pymetropolis.metro_pipeline.parameters import FloatParameter, ListParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_pipeline.types import String
from pymetropolis.metro_spatial import GeoStep, OSMStep
from pymetropolis.metro_spatial.simulation_area.file import SimulationAreaFile

from .file import UrbanAreasFile


def read_osm_urban_areas(
    osm_file: Path,
    landuse_tags: list[str],
    buffer: float,
    crs: pyproj.CRS,
    simulation_area_file: SimulationAreaFile,
):
    """Reads the areas with a urban landuse in the OSM file and returns a MultiPolygon representing
    all these urban areas.
    """
    filter_polygon = simulation_area_file.get_area_opt()
    logger.info("Reading urban areas")
    ids = list()
    tags = list()
    polygons = list()
    fab = WKBFactory()
    valid_tag_pairs = tuple(("landuse", tag) for tag in landuse_tags)
    logger.debug("Reading areas from OSM file")
    for area in (
        osmium.FileProcessor(osm_file).with_filter(TagFilter(*valid_tag_pairs)).with_areas()
    ):
        if area.is_area():  # ty: ignore[unresolved-attribute]
            ids.append(area.id)
            tags.append(area.tags["landuse"])
            polygons.append(fab.create_multipolygon(area))  # ty: ignore[invalid-argument-type]
    logger.debug("Building GeoDataFrame")
    gdf = gpd.GeoDataFrame(
        {"osm_id": ids, "landuse": tags}, geometry=gpd.GeoSeries.from_wkb(polygons, crs="EPSG:4326")
    )
    logger.debug("Converting to required CRS")
    gdf.to_crs(crs, inplace=True)
    if filter_polygon is not None:
        logger.debug("Filtering based on area")
        mask = [filter_polygon.intersects(geom) for geom in gdf.geometry]
        gdf = gdf.loc[mask].copy()
    logger.debug("Computing union of all urban areas")
    urban_area = gdf.union_all()
    logger.debug("Buffering and simplifying geometry")
    urban_area = urban_area.buffer(buffer).simplify(0, preserve_topology=False)
    gdf = gpd.GeoDataFrame(geometry=[urban_area])
    return gdf


class OpenStreetMapUrbanAreasStep(GeoStep, OSMStep):
    """Identifies urban areas from OpenStreetMap data.

    Urban areas are read from the OpenStreetMap areas with tag
    [landuse:*](https://wiki.openstreetmap.org/wiki/Key:landuse).

    The [`osm_urban_areas.urban_landuse_tags`](parameters.md#osm_urban_areasurban_landuse_tags)
    parameter is used to define the `landuse` values which are considered as urban.
    For example, values `"residential"`, `"industrial"`, `"commercial"`, or `"retail"` should
    generally be included.

    In addition to the `urban_landuse_tags` parameter, this step requires both the
    [`osm_file`](parameters.md#osm_file) and [`crs`](parameters.md#crs) parameters to be set.

    For example, to identifies the urban areas of Brittany, you can use:

    ```toml
    osm_file = "path/to/brittany.osm.pbf"
    crs = "epsg:2154"

    [osm_urban_areas]
    urban_landuse_tags = [
      "residential",
      "industrial",
      "commercial",
      "retail",
    ]
    ```

    The [`osm_urban_areas.buffer`](parameters.md#osm_urban_areasbuffer) parameter can be used to
    extend (positive values) or shrink (negative values) the areas.
    """

    urban_landuse_tags = ListParameter(
        "osm_urban_areas.urban_landuse_tags",
        inner=String(),
        min_length=1,
        description="List of `landuse=*` OpenStreetMap tags that define urban areas.",
        example='`["residential", "industrial", "commercial", "retail"]`',
        note=(
            "A list of landuse tags with description is available on the "
            "[OpenStreetMap wiki](https://wiki.openstreetmap.org/wiki/Key:landuse)."
        ),
    )
    buffer = FloatParameter(
        "osm_urban_areas.buffer",
        default=0.0,
        description="Distance by which the polygons of the urban areas are buffered.",
        note=(
            "The value is expressed in the unit of measure of the CRS (usually meter). "
            "Positive values extend the area, while negative values shrink it."
        ),
    )

    input_files = {"simulation_area": InputFile(SimulationAreaFile, optional=True)}
    output_files = {"urban_areas": UrbanAreasFile}

    def is_defined(self) -> bool:
        return (
            self.crs is not None
            and self.osm_file is not None
            and self.urban_landuse_tags is not None
        )

    def run(self):
        gdf = read_osm_urban_areas(
            osm_file=self.osm_file,
            landuse_tags=self.urban_landuse_tags,
            buffer=self.buffer,
            crs=self.crs,
            simulation_area_file=self.input["simulation_area"],  # ty: ignore[invalid-argument-type]
        )
        self.output["urban_areas"].write(gdf)
