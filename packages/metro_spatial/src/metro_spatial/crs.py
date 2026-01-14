from typing import Any

import pyproj
from metro_common.types import MetroType
from metro_pipeline import ConfigValue
from pyproj.exceptions import CRSError


class ProjectedCrs(MetroType):
    def check(self, value: Any):
        try:
            crs = pyproj.CRS.from_user_input(value)
        except CRSError:
            return False
        else:
            return crs.is_projected


CRS = ConfigValue(
    "crs",
    "crs",
    expected_type=ProjectedCrs,
    description="Projected coordinate system to be used for spatial operations.",
    example='"EPSG:2154" (Lambert projection adapted for France)',
    note=(
        "You can use the epsg.io website to find a projected coordinate system that is adapted for "
        "your study area. It is strongly recommended that the unit of measure is meter. If you use "
        "a coordinate system for an area of use that is not adapted or with an incorrect unit of "
        "measure, then some operations might fail or the results might be erroneous (like road "
        "length being overestimated)."
    ),
)
