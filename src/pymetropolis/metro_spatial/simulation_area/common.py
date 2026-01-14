from typing import Any

import geopandas as gpd
from loguru import logger
from shapely import Geometry


def buffer_area(geom: Geometry, buffer: float) -> Geometry:
    logger.debug("Buffering the polygon")
    geom = geom.buffer(buffer, cap_style="square")
    return geom


def geom_as_gdf(geom: Geometry, crs: Any) -> gpd.GeoDataFrame:
    area = geom.area / 1e6
    logger.info(f"Area of the polygon: {area:,.0f} km²")
    return gpd.GeoDataFrame(geometry=[geom], crs=crs)
