from typing import Any

import geopandas as gpd
import osmium
from loguru import logger
from osmium.filter import TagFilter
from osmium.geom import WKBFactory
from osmium.osm import Area

from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_pipeline.parameters import CustomParameter, FloatParameter, IntParameter
from pymetropolis.metro_spatial import GeoMetroStep, OSMMetroStep

from .common import buffer_area, geom_as_gdf
from .file import SimulationAreaFile


def name_or_names_validator(value: Any) -> str | list[str]:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        if all(isinstance(v, str) for v in value):
            return value
        else:
            raise MetropyError(f"List items are not all string: `{value}`")
    raise MetropyError(f"Invalid value (expected string or list of strings): `{value}`")


class SimulationAreaFromOSMStep(GeoMetroStep, OSMMetroStep):
    osm_name = CustomParameter(
        "simulation_area.osm_name",
        validator=name_or_names_validator,
        description="List of subdivision names to be considered when reading administrative boundaries.",
        example='`"Madrid"`',
        note=(
            "The values are compared with the `name=*` tag of the OpenStreetMap features. Be careful, "
            "the name can sometimes be in the local language."
        ),
    )
    osm_admin_level = IntParameter(
        "simulation_area.osm_admin_level",
        description="Administrative level to be considered when reading administrative boundaries.",
        note=(
            "See https://wiki.openstreetmap.org/wiki/Tag:boundary%3Dadministrative#Table_:_Admin_level_for_all_countries "
            "for a table with the meaning of all possible value for each country."
        ),
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
        return (
            self.crs is not None
            and self.osm_file is not None
            and self.osm_name is not None
            and self.osm_admin_level is not None
        )

    def run(self):
        names = self.osm_name
        if len(names) == 0:
            raise MetropyError("You must provide at least one name to be selected")
        if isinstance(names, str):
            # Only one name provided.
            name_pairs = (("name", names),)
        else:
            name_pairs = tuple(("name", name) for name in names)
        fab = WKBFactory()
        logger.debug("Reading areas from OSM file")
        found_names = list()
        polygons = list()
        for area in (
            osmium.FileProcessor(self.osm_file)
            .with_filter(TagFilter(("admin_level", str(self.osm_admin_level))))
            .with_filter(TagFilter(*name_pairs))
            .with_areas()
        ):
            assert isinstance(area, Area)
            if area.is_area():
                found_names.append(area.tags["name"])
                polygons.append(fab.create_multipolygon(area))
        if not found_names:
            raise MetropyError(
                f"The OpenStreetMap data does not contain any relation with \
                `admin_level={self.osm_admin_level}` and `name` in `{names}`"
            )
        logger.debug("Building GeoDataFrame")
        gdf = gpd.GeoDataFrame(
            {"name": names}, geometry=gpd.GeoSeries.from_wkb(polygons, crs="EPSG:4326")
        )
        missing_names = set(names).difference(set(gdf["name"]))
        if missing_names:
            logger.warning(f"No relation was found for the following names: {missing_names}")
        logger.debug("Converting to required CRS")
        gdf.to_crs(self.crs, inplace=True)
        geom = gdf.union_all()
        if self.buffer != 0.0:
            geom = buffer_area(geom, self.buffer)
        gdf = geom_as_gdf(geom, self.crs)
        self.output["simulation_area"].write(gdf)
