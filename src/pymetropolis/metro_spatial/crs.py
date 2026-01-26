from typing import Any

import pyproj
from pyproj.exceptions import CRSError

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_pipeline.parameters import CustomParameter
from pymetropolis.metro_pipeline.steps import MetroStep


def validate_crs(value: Any) -> pyproj.CRS:
    try:
        crs = pyproj.CRS.from_user_input(value)
    except CRSError:
        raise MetropyError(f"Invalid CRS: `{value}`")
    if crs.is_projected:
        return crs
    else:
        raise MetropyError(f"CRS is valid but is not projected: `{value}`")


class GeoMetroStep(MetroStep):
    crs = CustomParameter(
        "crs",
        validator=validate_crs,
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
