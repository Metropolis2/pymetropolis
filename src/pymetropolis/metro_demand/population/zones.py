import geopandas as gpd
import polars as pl
import requests
from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_spatial import GeoStep

from .files import (
    HouseholdsHomesFile,
    HouseholdsZonesFile,
    TripsDestinationsFile,
    TripsOriginsFile,
    TripsZonesFile,
)

API_URL = "https://data.geopf.fr/wfs"
API_PARAMS = {
    "service": "WFS",
    "version": "2.0.0",
    "request": "GetFeature",
    "outputFormat": "application/json",
    "srsname": "EPSG:4326",
}


def get_ign_data(
    name: str, columns: list[str], bbox: tuple[float, float, float, float] | None = None
):
    params = API_PARAMS.copy()
    params["typeNames"] = name
    if bbox is not None:
        min_lng, min_lat, max_lng, max_lat = bbox
        params["CQL_FILTER"] = f"BBOX(geometrie, {min_lat}, {min_lng}, {max_lat}, {max_lng})"
    try:
        response = requests.get(API_URL, params=params)
    except Exception as e:
        raise MetropyError(f"Failed to request data from {API_URL}:\n{e}")
    if response.ok:
        data = response.json()
        if "totalFeatures" not in data or data["totalFeatures"] == 0:
            raise MetropyError(
                "No feature returned from the IGN WFS API. Is the simulation area in France?"
            )
        gdf = gpd.GeoDataFrame.from_features(
            data["features"], columns=["geometry", *columns], crs=data["crs"]["properties"]["name"]
        )
        return gdf
    else:
        logger.error(response.content)
        raise MetropyError("Cannot read from IGN WFS API ({name}).")


def get_iris(bbox: tuple[float, float, float, float] | None = None):
    return get_ign_data("STATISTICALUNITS.IRIS.PE:contours_iris_pe", ["code_iris"], bbox)


def get_insee(bbox: tuple[float, float, float, float] | None = None):
    return get_ign_data(
        "ADMINEXPRESS-COG-CARTO.LATEST:commune",
        ["code_insee", "code_insee_du_departement", "code_insee_de_la_region"],
        bbox,
    )


def identify_iris(points: gpd.GeoDataFrame, iris: gpd.GeoDataFrame, id_col: str) -> pl.DataFrame:
    gdf = gpd.sjoin(points, iris.to_crs(points.crs), predicate="intersects", how="left")
    df = pl.from_pandas(gdf.loc[:, [id_col, "code_iris"]])
    df = df.sort(id_col, "code_iris")
    # Drop duplicates (in case of ties).
    df = df.unique(subset=id_col, keep="first", maintain_order=True)
    return df


def identify_insee(points: gpd.GeoDataFrame, insee: gpd.GeoDataFrame, id_col: str) -> pl.DataFrame:
    gdf = gpd.sjoin(points, insee.to_crs(points.crs), predicate="intersects", how="left")
    df = pl.from_pandas(
        gdf.loc[:, [id_col, "code_insee", "code_insee_du_departement", "code_insee_de_la_region"]]
    )
    df = df.sort(id_col, "code_insee")
    # Drop duplicates (in case of ties).
    df = df.unique(subset=id_col, keep="first", maintain_order=True)
    return df


class FrenchHouseholdsHomesZonesStep(GeoStep):
    """Identify where the households' homes are located in the French zoning system."""

    input_files = {"homes": HouseholdsHomesFile}
    output_files = {"zones": HouseholdsZonesFile}

    def run(self):
        homes = self.input["homes"].read()
        bbox = homes.geometry.to_crs("EPSG:4326").total_bounds

        iris = get_iris(bbox=bbox)
        insee = get_insee(bbox=bbox)

        df_with_iris = identify_iris(homes, iris, "household_id")
        df_with_insee = identify_insee(homes, insee, "household_id")
        df = df_with_iris.join(df_with_insee, on="household_id")
        df = df.rename(
            {
                "code_iris": "home_zone5",
                "code_insee": "home_zone4",
                "code_insee_du_departement": "home_zone2",
                "code_insee_de_la_region": "home_zone1",
            }
        )
        self.output["zones"].write(df)


class FrenchTripsZonesStep(GeoStep):
    """Identify where the trips' origins are located in the French zoning system."""

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

        iris = get_iris(bbox)
        insee = get_insee(bbox)

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
                "code_iris": "origin_zone5",
                "code_insee": "origin_zone4",
                "code_insee_du_departement": "origin_zone2",
                "code_insee_de_la_region": "origin_zone1",
                "code_iris_dest": "destination_zone5",
                "code_insee_dest": "destination_zone4",
                "code_insee_du_departement_dest": "destination_zone2",
                "code_insee_de_la_region_dest": "destination_zone1",
            }
        )
        self.output["zones"].write(df)
