import geopandas as gpd
import polars as pl

from pymetropolis.metro_spatial.ign import AdminExpressStep, IRISStep

from .files import (
    HouseholdsHomesFile,
    HouseholdsZonesFile,
    TripsDestinationsFile,
    TripsOriginsFile,
    TripsZonesFile,
)

# TODO: Add EPCI zone3.


def identify_iris(points: gpd.GeoDataFrame, iris: gpd.GeoDataFrame, id_col: str) -> pl.DataFrame:
    gdf = gpd.sjoin(points, iris.to_crs(points.crs), predicate="intersects", how="left")
    df = pl.from_pandas(gdf.loc[:, [id_col, "iris_id"]])
    df = df.sort(id_col, "iris_id")
    # Drop duplicates (in case of ties).
    df = df.unique(subset=id_col, keep="first", maintain_order=True)
    return df


def identify_insee(
    points: gpd.GeoDataFrame, communes: gpd.GeoDataFrame, id_col: str
) -> pl.DataFrame:
    gdf = gpd.sjoin(points, communes.to_crs(points.crs), predicate="intersects", how="left")
    df = pl.from_pandas(gdf.loc[:, [id_col, "insee_id", "departement_id", "region_id"]])
    df = df.sort(id_col, "insee_id")
    # Drop duplicates (in case of ties).
    df = df.unique(subset=id_col, keep="first", maintain_order=True)
    return df


class FrenchHouseholdsHomesZonesStep(AdminExpressStep, IRISStep):
    """Identifies the geographic zones where households' homes are located in the French zoning
    system.

    The French zoning system uses multiple levels of geographic zones:

    - Zone 1: Region
    - Zone 2: Department
    - Zone 3: EPCI (Établissement Public de Coopération Intercommunale)
    - Zone 4: INSEE commune
    - Zone 5: IRIS

    Check the [`AdminExpressStep`](steps.md#adminexpressstep) and [`IRISStep`](steps.md#irisstep)
    abstract steps to know how to configure this step.
    """

    input_files = {"homes": HouseholdsHomesFile}
    output_files = {"zones": HouseholdsZonesFile}

    def run(self):
        homes = self.input["homes"].read()
        bbox = tuple(homes.geometry.to_crs("EPSG:4326").total_bounds)

        iris = self.read_iris(bbox=bbox)
        communes = self.read_communes(bbox=bbox)

        df_with_iris = identify_iris(homes, iris, "household_id")
        df_with_insee = identify_insee(homes, communes, "household_id")
        df = df_with_iris.join(df_with_insee, on="household_id")
        df = df.rename(
            {
                "iris_id": "home_zone5",
                "insee_id": "home_zone4",
                "departement_id": "home_zone2",
                "region_id": "home_zone1",
            }
        )
        self.output["zones"].write(df)


class FrenchTripsZonesStep(AdminExpressStep, IRISStep):
    """Identifies the geographic zones where trips' origins and destinations are located in the
    French zoning system.

    The French zoning system uses multiple levels of geographic zones:

    - Zone 1: Region
    - Zone 2: Department
    - Zone 3: EPCI (Établissement Public de Coopération Intercommunale)
    - Zone 4: INSEE commune
    - Zone 5: IRIS

    Check the [`AdminExpressStep`](steps.md#adminexpressstep) and [`IRISStep`](steps.md#irisstep)
    abstract steps to know how to configure this step.
    """

    input_files = {"origins": TripsOriginsFile, "destinations": TripsDestinationsFile}
    output_files = {"zones": TripsZonesFile}

    def run(self):
        origins = self.input["origins"].read()
        destinations = self.input["destinations"].read()
        obbox = origins.geometry.to_crs("EPSG:4326").total_bounds
        dbbox = destinations.geometry.to_crs("EPSG:4326").total_bounds
        minx = min(obbox[0], dbbox[0])
        miny = min(obbox[1], dbbox[1])
        maxx = max(obbox[2], dbbox[2])
        maxy = max(obbox[3], dbbox[3])
        bbox = (minx, miny, maxx, maxy)

        iris = self.read_iris(bbox)
        insee = self.read_communes(bbox)

        origins_with_iris = identify_iris(origins, iris, "trip_id")
        origins_with_insee = identify_insee(origins, insee, "trip_id")
        destinations_with_iris = identify_iris(destinations, iris, "trip_id")
        destinations_with_insee = identify_insee(destinations, insee, "trip_id")
        df = (
            origins_with_iris.join(origins_with_insee, on="trip_id")
            .join(destinations_with_iris, on="trip_id", suffix="_dest")
            .join(destinations_with_insee, on="trip_id", suffix="_dest")
        )
        df = df.rename(
            {
                "iris_id": "origin_zone5",
                "insee_id": "origin_zone4",
                "departement_id": "origin_zone2",
                "region_id": "origin_zone1",
                "iris_id_dest": "destination_zone5",
                "insee_id_dest": "destination_zone4",
                "departement_id_dest": "destination_zone2",
                "region_id_dest": "destination_zone1",
            }
        )
        self.output["zones"].write(df)
