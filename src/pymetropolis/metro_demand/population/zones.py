from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_demand.zones.file import (
    ZonesLevel1File,
    ZonesLevel2File,
    ZonesLevel3File,
    ZonesLevel4File,
    ZonesLevel5File,
)
from pymetropolis.metro_pipeline import PopulationStep
from pymetropolis.metro_pipeline.steps import InputFile

from .files import (
    HouseholdsHomesFile,
    HouseholdsZonesFile,
    TripsDestinationsFile,
    TripsOriginsFile,
    TripsZonesFile,
)

if TYPE_CHECKING:
    import geopandas as gpd


def identify_zones(
    points: gpd.GeoDataFrame, zones: gpd.GeoDataFrame, id_col: str, name: str, level: int
) -> gpd.GeoDataFrame:
    import geopandas as gpd

    gdf = gpd.sjoin(zones[["zone_id", "geometry"]], points, predicate="intersects", how="right")
    gdf.drop(columns="index_left", inplace=True)
    gdf.rename(columns={"zone_id": f"{name}_zone{level}"}, inplace=True)
    # Drop duplicates (in case of ties).
    gdf.drop_duplicates(subset=id_col, inplace=True, ignore_index=True)
    return gdf


class HouseholdsHomesZonesStep(PopulationStep):
    """Identifies the geographic zones where households' homes are located."""

    input_files = {
        "homes": HouseholdsHomesFile,
        "zones1": InputFile(ZonesLevel1File, optional=True),
        "zones2": InputFile(ZonesLevel2File, optional=True),
        "zones3": InputFile(ZonesLevel3File, optional=True),
        "zones4": InputFile(ZonesLevel4File, optional=True),
        "zones5": InputFile(ZonesLevel5File, optional=True),
    }
    output_files = {"home_zones": HouseholdsZonesFile}

    def run(self):
        import polars as pl

        homes = self.input["homes"].read()

        for lvl in range(1, 6):
            zones = self.input[f"zones{lvl}"].read_if_exists()
            if zones is None:
                continue
            logger.debug(f"Identifying home zones for level {lvl} zones")
            homes = identify_zones(homes, zones, id_col="household_id", name="home", level=lvl)

        # Note. If no `zonesX` file is defined, this will just save a DataFrame with `household_id`.
        df = pl.from_pandas(homes.drop(columns="geometry"))
        self.output["home_zones"].write(df)


class TripsZonesStep(PopulationStep):
    """Identifies the geographic zones where trips' origins and destinations are located."""

    input_files = {
        "origins": TripsOriginsFile,
        "destinations": TripsDestinationsFile,
        "zones1": InputFile(ZonesLevel1File, optional=True),
        "zones2": InputFile(ZonesLevel2File, optional=True),
        "zones3": InputFile(ZonesLevel3File, optional=True),
        "zones4": InputFile(ZonesLevel4File, optional=True),
        "zones5": InputFile(ZonesLevel5File, optional=True),
    }
    output_files = {"trip_zones": TripsZonesFile}
    priority = 0

    def run(self):
        import polars as pl

        origins = self.input["origins"].read()
        destinations = self.input["destinations"].read()

        for lvl in range(1, 6):
            zones = self.input[f"zones{lvl}"].read_if_exists()
            if zones is None:
                continue
            logger.debug(f"Identifying origin zones for level {lvl} zones")
            origins = identify_zones(origins, zones, id_col="trip_id", name="origin", level=lvl)
            logger.debug(f"Identifying destination zones for level {lvl} zones")
            destinations = identify_zones(
                destinations, zones, id_col="trip_id", name="destination", level=lvl
            )

        # Note. If no `zonesX` file is defined, this will just save a DataFrame with `trip_id`.
        df = pl.from_pandas(origins.drop(columns="geometry")).join(
            pl.from_pandas(destinations.drop(columns="geometry")), on="trip_id"
        )
        self.output["trip_zones"].write(df)
