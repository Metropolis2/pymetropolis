from pymetropolis.metro_demand.population.files import (
    HouseholdsHomesFile,
    TripsDestinationsFile,
    TripsOriginsFile,
)
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import BoolParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_spatial import GeoStep
from pymetropolis.metro_spatial.ign import AdminExpressStep, IRISStep
from pymetropolis.metro_spatial.simulation_area.file import SimulationAreaFile

from .file import ZonesLevel1File, ZonesLevel2File, ZonesLevel3File, ZonesLevel4File


class AbstractFrenchZonesStep(Step):
    """Abstract Step with the `zones.france` parameter indicating whether the simulation is in
    France and can use the French zoning system.
    """

    enabled = BoolParameter(
        "zones.france",
        default=False,
        description=(
            "Whether the French zoning system should be used to defined the simulation's zones."
        ),
    )

    def is_defined(self) -> bool:
        return self.enabled is True


class FrenchZonesStep(AbstractFrenchZonesStep, IRISStep, AdminExpressStep, GeoStep):
    """Reads zones from France data.

    The French zoning system uses multiple levels of geographic zones:

    - Zone 1: Region
    - Zone 2: Department
    - Zone 3: INSEE commune
    - Zone 4: IRIS

    The fifth level is free to be used as a custom level, with the
    [`CustomZonesLevel5Step`](steps.md#customzoneslevel5step).
    """

    input_files = {
        "area": SimulationAreaFile,
        "origins": InputFile(TripsOriginsFile, optional=True, all_populations=True),
        "destinations": InputFile(TripsDestinationsFile, optional=True, all_populations=True),
        "homes": InputFile(HouseholdsHomesFile, optional=True, all_populations=True),
    }
    output_files = {
        "zones1": ZonesLevel1File,
        "zones2": ZonesLevel2File,
        "zones3": ZonesLevel3File,
        "zones4": ZonesLevel4File,
    }

    def run(self):
        import geopandas as gpd
        import pandas as pd

        area = self.input["area"].get_area(crs="EPSG:4326")  # ty: ignore[unresolved-attribute]

        # Read all simulation locations (origin, destination, home), for every population, if
        # they are defined.
        all_points = gpd.GeoSeries([], crs="EPSG:4326")
        for loc in ("origins", "destinations", "homes"):
            for f in self.input_populations[loc].values():
                if f.exists():
                    gdf = f.read()
                    all_points = pd.concat((all_points, gdf.geometry.to_crs("EPSG:4326")))

        # Compute the largest bbox that should be retrieved.
        minx, miny, maxx, maxy = area.bounds
        if not all_points.empty:
            pminx, pminy, pmaxx, pmaxy = all_points.total_bounds
            minx = min(minx, pminx)
            miny = min(miny, pminy)
            maxx = max(maxx, pmaxx)
            maxy = max(maxy, pmaxy)
        bbox = (minx, miny, maxx, maxy)

        for i, (func, id_col) in enumerate(
            (
                (self.read_regions, "region_id"),
                (self.read_departements, "departement_id"),
                (self.read_communes, "insee_id"),
                (self.read_iris, "iris_id"),
            )
        ):
            gdf = func(bbox=bbox)
            gdf["within_area"] = gdf.intersects(area)
            intersects_points = (
                gdf.intersects(all_points.union_all()) if not all_points.empty else False
            )
            # Filter zones that are either within the area or contain at least one location.
            gdf = gdf[gdf["within_area"] | intersects_points]
            gdf = (
                gdf[["geometry", id_col, "name", "within_area"]]
                .copy()
                .rename(columns={id_col: "zone_id"})
                .to_crs(self.crs)
            )
            self.output[f"zones{i + 1}"].write(gdf)
