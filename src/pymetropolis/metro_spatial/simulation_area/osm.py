import geopandas as gpd
import osmium
from loguru import logger
from osmium.filter import TagFilter
from osmium.geom import WKBFactory
from osmium.osm import Area

from pymetropolis.metro_common.errors import MetropyError, error_context
from pymetropolis.metro_pipeline import Config, ConfigTable, ConfigValue, Step
from pymetropolis.metro_spatial import CRS, OSM_FILE

from .common import buffer_area, geom_as_gdf
from .file import SIMULATION_AREA_FILE

OSM_ADMIN_LEVEL = ConfigValue(
    "simulation_area.osm_admin_level",
    key="osm_admin_level",
    expected_type=int,
    description="Administrative level to be considered when reading administrative boundaries.",
    example="`6`",
    note=(
        "See https://wiki.openstreetmap.org/wiki/Tag:boundary%3Dadministrative#Table_:_Admin_level_for_all_countries "
        "for a table with the meaning of all possible value for each country."
    ),
)

OSM_NAME = ConfigValue(
    "simulation_area.osm_name",
    key="osm_name",
    expected_type=str | list[str],
    description="List of subdivision names to be considered when reading administrative boundaries.",
    example='`"Madrid"`',
    note=(
        "The values are compared with the `name=*` tag of the OpenStreetMap features. Be careful, "
        "the name can sometimes be in the local language."
    ),
)

BUFFER = ConfigValue(
    "simulation_area.buffer",
    key="buffer",
    expected_type=float,
    default=0.0,
    description="Distance by which the polygon of the simulation area must be extended or shrinked.",
    example="`500`",
    note=(
        "The value is expressed in the unit of measure of the CRS (usually meter). Positive values "
        "extend the area, while negative values shrink it."
    ),
)

SIMULATION_AREA_FROM_OSM_TABLE = ConfigTable(
    "simulation_area_from_osm",
    "simulation_area_from_osm",
    items=[OSM_ADMIN_LEVEL, OSM_NAME, BUFFER],
    description=(
        "Creates a polygon of the simulation area from OpenStreetMap administrative boundaries."
    ),
)

OSM_CONFIG = [SIMULATION_AREA_FROM_OSM_TABLE, OSM_ADMIN_LEVEL, OSM_NAME, BUFFER]


@error_context(msg="Cannot create simulation area from OpenStreetMap data")
def from_osm(config: Config):
    logger.info("Reading polygon from OpenStreetMap administrative boundaries")
    admin_level = config[OSM_ADMIN_LEVEL]
    names = config[OSM_NAME]
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
        osmium.FileProcessor(config[OSM_FILE])
        .with_filter(TagFilter(("admin_level", str(admin_level))))
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
            `admin_level={admin_level}` and `name` in `{names}`"
        )
    logger.debug("Building GeoDataFrame")
    gdf = gpd.GeoDataFrame(
        {"name": names}, geometry=gpd.GeoSeries.from_wkb(polygons, crs="EPSG:4326")
    )
    missing_names = set(names).difference(set(gdf["name"]))
    if missing_names:
        logger.warning(f"No relation was found for the following names: {missing_names}")
    logger.debug("Converting to required CRS")
    gdf.to_crs(config[CRS], inplace=True)
    geom = gdf.union_all()
    if config[BUFFER] != 0.0:
        geom = buffer_area(geom, config[BUFFER])
    gdf = geom_as_gdf(geom, config[CRS])
    SIMULATION_AREA_FILE.save(gdf, config)
    return True


SIMULATION_AREA_FROM_OSM = Step(
    "simulation-area-from-osm",
    from_osm,
    output_files=[SIMULATION_AREA_FILE],
    config_values=[CRS, OSM_FILE, OSM_NAME, OSM_ADMIN_LEVEL, BUFFER],
)
