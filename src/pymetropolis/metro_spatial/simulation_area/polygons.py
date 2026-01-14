from loguru import logger

from pymetropolis.metro_common.errors import error_context
from pymetropolis.metro_common.io import read_geodataframe
from pymetropolis.metro_common.types import Path
from pymetropolis.metro_pipeline import Config, ConfigTable, ConfigValue, Step
from pymetropolis.metro_spatial import CRS

from .common import buffer_area, geom_as_gdf
from .file import SIMULATION_AREA_FILE

POLYGON_FILE = ConfigValue(
    "simulation_area.polygon_file",
    key="polygon_file",
    expected_type=Path(),
    description="Path to the geospatial file containing polygon(s) of the simulation area.",
    example='`"data/my_area.geojson"`',
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

SIMULATION_AREA_FROM_POLYGONS_TABLE = ConfigTable(
    "simulation_area_from_polygons",
    "simulation_area_from_polygons",
    items=[POLYGON_FILE, BUFFER],
    description="Creates the simulation area from a set of polygon(s).",
)

POLYGONS_CONFIG = [SIMULATION_AREA_FROM_POLYGONS_TABLE, POLYGON_FILE, BUFFER]


@error_context(msg="Cannot read simulation area from polygons")
def from_polygons(config: Config):
    logger.info("Reading polygon from geospatial file")
    filename = config[POLYGON_FILE]
    gdf = read_geodataframe(filename, columns=["geometry"])
    gdf.to_crs(config[CRS], inplace=True)
    geom = gdf.union_all()
    if config[BUFFER] != 0.0:
        geom = buffer_area(geom, config[BUFFER])
    gdf = geom_as_gdf(geom, config[CRS])
    SIMULATION_AREA_FILE.save(gdf, config)
    return True


SIMULATION_AREA_FROM_POLYGONS = Step(
    "simulation-area-from-polygons",
    from_polygons,
    output_files=[SIMULATION_AREA_FILE],
    config_values=[CRS, POLYGON_FILE, BUFFER],
)
