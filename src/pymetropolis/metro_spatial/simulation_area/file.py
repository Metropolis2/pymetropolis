from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_pipeline.file import MetroGeoDataFrameFile

if TYPE_CHECKING:
    from shapely.geometry import Polygon


class SimulationAreaFile(MetroGeoDataFrameFile):
    path = "areas/simulation_area.geo.parquet"
    description = "Single-feature file with the geometry of the simulation area."
    max_rows = 1

    def get_area(self) -> Polygon:
        """Returns the simulation area as a Polygon.

        If the file does not exist, raises an error."""
        from shapely.geometry import Polygon

        gdf = self.read()
        area = gdf["geometry"].iloc[0]
        assert isinstance(area, Polygon)
        return area

    def get_area_opt(self) -> Polygon | None:
        """Returns the simulation area as a Polygon.

        If the file does not exist, returns None."""
        if not self.exists():
            return None
        return self.get_area()
