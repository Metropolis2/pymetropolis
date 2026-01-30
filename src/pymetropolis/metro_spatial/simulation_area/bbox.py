import geopandas as gpd
import shapely

from pymetropolis.metro_pipeline.parameters import (
    BoolParameter,
    ListParameter,
)
from pymetropolis.metro_pipeline.types import Float
from pymetropolis.metro_spatial import GeoStep

from .file import SimulationAreaFile


class SimulationAreaFromBboxStep(GeoStep):
    """Creates the simulation area from a bounding box.

    This is the easiest way to specify the simulation area. However, the area is thus limited to
    rectangles.

    The bounding box needs to be specified as the `bbox` value, which expects a list of coordinates
    `[minx, miny, maxx, maxy]`.

    By default, the coordinates need to be specified in the simulation CRS.
    If you want to specify them in WGS84 (longitude, latitude), you can use `bbox_wgs = true`.

    You can go to [www.openstreetmap.org](https://www.openstreetmap.org) to identify the bounding
    box of a region (as WGS84 coordinates), using the "Export" button.

    ```toml
    [simulation_area]
    bbox = [1.4777, 48.3955, 3.6200, 49.2032]
    bbox_wgs = true
    ```
    """

    bbox = ListParameter(
        "simulation_area.bbox",
        inner=Float(),
        length=4,
        description="Bounding box to be used as simulation area.",
        example="`[1.4777, 48.3955, 3.6200, 49.2032]`",
        note=(
            "Note: The values need to be specified as [minx, miny, maxx, maxy], in the simulation’s "
            "CRS. If bbox_wgs = true, the values need to be specified in WGS 84 (longitude, latitude)."
        ),
    )
    bbox_wgs = BoolParameter(
        "simulation_area.bbox_wgs",
        default=False,
        description=(
            "Whether the `bbox` values are specified in the simulation CRS (`false`) or in WGS84 "
            "(`true`)"
        ),
    )
    output_files = {"simulation_area": SimulationAreaFile}

    def is_defined(self) -> bool:
        return self.crs is not None and self.bbox is not None

    def run(self):
        box = shapely.box(*self.bbox)
        if self.bbox_wgs:
            # Convert the box to the simulation's CRS.
            gdf = gpd.GeoSeries([box], crs="EPSG:4326")
            box = gdf.to_crs(self.crs).iloc[0]
        gdf = gpd.GeoDataFrame(geometry=[box], crs=self.crs)
        self.output["simulation_area"].write(gdf)
