import geopandas as gpd
import shapely
from loguru import logger
from metro_common.errors import error_context
from metro_common.types import FixedSizeList
from metro_pipeline import Config, ConfigTable, ConfigValue, Step

from metro_spatial import CRS

from .file import SIMULATION_AREA_FILE

BBOX = ConfigValue(
    "simulation_area.bbox",
    key="bbox",
    expected_type=FixedSizeList(4, float),
    description="Bounding box to be used as simulation area.",
    example="`[1.4777, 48.3955, 3.6200, 49.2032]`",
    note=(
        "Note: The values need to be specified as [minx, miny, maxx, maxy], in the simulation’s "
        "CRS. If bbox_wgs = true, the values need to be specified in WGS 84 (longitude, latitude)."
    ),
)

BBOX_WGS = ConfigValue(
    "simulation_area.bbox_wgs",
    key="bbox_wgs",
    expected_type=bool,
    default=False,
    description=(
        "Whether the `bbox` values are specified in the simulation CRS (`false`) or in WGS84 "
        "(`true`)"
    ),
    example="`true`",
)

SIMULATION_AREA_FROM_BBOX_TABLE = ConfigTable(
    "simulation_area_from_bbox",
    "simulation_area_from_bbox",
    items=[BBOX, BBOX_WGS],
    description="Creates a polygon of the simulation area from a bounding box.",
)

BBOX_CONFIG = [SIMULATION_AREA_FROM_BBOX_TABLE, BBOX, BBOX_WGS]


@error_context(msg="Cannot create simulation area from bounding box")
def from_bbox(config: Config):
    logger.info("Creating polygon from a bounding box")
    bbox = config[BBOX]
    box = shapely.box(*bbox)
    if config[BBOX_WGS]:
        # Convert the box to the simulation's CRS.
        gdf = gpd.GeoSeries([box], crs="EPSG:4326")
        box = gdf.to_crs(config[CRS]).iloc[0]
    gdf = gpd.GeoDataFrame(geometry=[bbox], crs=config[CRS])
    SIMULATION_AREA_FILE.save(gdf, config)
    return True


SIMULATION_AREA_FROM_BBOX = Step(
    "simulation-area-from-bbox",
    from_bbox,
    output_files=[SIMULATION_AREA_FILE],
    config_values=[CRS, BBOX, BBOX_WGS],
)
