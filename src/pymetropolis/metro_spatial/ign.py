import time

import geopandas as gpd
import requests
from loguru import logger
from shapely.geometry import box

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.utils import find_file
from pymetropolis.metro_pipeline.parameters import PathParameter, StringParameter
from pymetropolis.metro_pipeline.steps import Step

# Maximum number of retries when the API request fails.
MAX_RETRIES = 3


class IGNStep(Step):
    """Abstract Step to make calls to the IGN WFS API."""

    api_wfs_url = StringParameter(
        "ign.api_wfs_url",
        default="https://data.geopf.fr/wfs",
        description="Url to the WFS API from which to get IGN data.",
    )

    def default_api_params(self) -> dict[str, str]:
        return {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "outputFormat": "application/json",
            "srsname": "EPSG:4326",
        }

    def get_ign_data(
        self, name: str, columns: list[str], bbox: tuple[float, float, float, float] | None = None
    ) -> gpd.GeoDataFrame:
        """Requests IGN data from the WFS API and returns a GeoDataFrame with the results.

        - `name`: name of the API service from which to request data.
        - `columns`: column names to include in the final GeoDataFrame.
        - `bbox`: optional bbox (in WGS84, min_lng, min_lat, max_lng, max_lat) from which to
          restrict the geometries.
        """
        params = self.default_api_params()
        params["typeNames"] = name
        if bbox is not None:
            min_lng, min_lat, max_lng, max_lat = bbox
            params["CQL_FILTER"] = f"BBOX(geometrie, {min_lat}, {min_lng}, {max_lat}, {max_lng})"
        for i in range(MAX_RETRIES):
            try:
                response = requests.get(self.api_wfs_url, params=params)
            except Exception as e:
                # Store the exception, wait 1 second, then retry if MAX_RETRIES is not exceeded.
                logger.warning(
                    f"Request to {self.api_wfs_url} failed. Retrying in 1 second... "
                    f"Attempt {i + 1}/{MAX_RETRIES}. Error:\n{e}"
                )
                time.sleep(1)
            else:
                # Got a response.
                break
        else:
            # The for loop finished without success.
            raise MetropyError(
                f"Failed to request data from {self.api_wfs_url} after {MAX_RETRIES} attempts."
            )
        if response.ok:
            data = response.json()
            if "totalFeatures" not in data or data["totalFeatures"] == 0:
                raise MetropyError("No feature returned from the IGN WFS API.")
            gdf = gpd.GeoDataFrame.from_features(
                data["features"],
                columns=["geometry", *columns],
                crs=data["crs"]["properties"]["name"],
            )
            return gdf
        else:
            logger.error(response.content)
            raise MetropyError(f"Cannot read from IGN WFS API ({name}).")


class AdminExpressStep(IGNStep):
    """Abstract Step to retrieve data from the ADMIN EXPRESS database.

    Data is retrieved from local files when the
    [`admin_express_directory`](parameters.md#ignadmin_express_directory) parameter is defined,
    otherwise data is requested from the WFS API.
    """

    admin_express_directory = PathParameter(
        "ign.admin_express_directory",
        check_dir_exists=True,
        description="Path to the directory where the ADMIN EXPRESS database is stored.",
        note=(
            "The database can be downloaded from "
            "[cartes.gouv.fr](https://cartes.gouv.fr/rechercher-une-donnee/dataset/IGNF_ADMIN-EXPRESS)."
            "The ADMIN EXPRESS COG CARTO version and GPKG format are recommended. "
            "The 7z file needs to be extracted."
        ),
    )
    api_communes_service_name = StringParameter(
        "ign.api_communes_service_name",
        default="ADMINEXPRESS-COG-CARTO.LATEST:commune",
        description="Name of the API service from which to request communes data.",
        note='For faster but less accurate queries, you can use the "COG-CARTO-PE" version.',
    )

    def read_communes(
        self, bbox: tuple[float, float, float, float] | None = None
    ) -> gpd.GeoDataFrame:
        """Returns a GeoDataFrame of INSEE Communes read from the ADMIN EXPRESS database.

        When the `bbox` parameter is specified, only returns Communes that intersect that bbox.
        """
        if self.admin_express_directory is not None:
            global_file = find_file("ADE*.gpkg", self.admin_express_directory, recursive=True)
            # Order versions have a commune-specific file.
            commune_file = find_file("COMMUNE.shp", self.admin_express_directory, recursive=True)
            if bbox is None:
                mask = None
            else:
                # Put the bbox in a gpd.GeoSeries so that CRS mis-match are properly resolved.
                mask = gpd.GeoSeries([box(*bbox)], crs="EPSG:4326")
            if global_file is not None:
                communes = gpd.read_file(
                    global_file,
                    layer="commune",
                    columns=[
                        "geometry",
                        "code_insee",
                        "code_insee_du_departement",
                        "code_insee_de_la_region",
                        "nom_officiel",
                    ],
                    mask=mask,
                )
                communes.rename(
                    columns={
                        "code_insee": "insee_id",
                        "code_insee_du_departement": "departement_id",
                        "code_insee_de_la_region": "region_id",
                        "nom_officiel": "name",
                    },
                    inplace=True,
                )
            elif commune_file is not None:
                communes = gpd.read_file(
                    commune_file,
                    columns=["geometry", "INSEE_COM", "INSEE_DEP", "INSEE_REG", "NOM"],
                    mask=mask,
                )
                communes.rename(
                    columns={
                        "INSEE_COM": "insee_id",
                        "INSEE_DEP": "departement_id",
                        "INSEE_REG": "region_id",
                        "NOM": "name",
                    },
                    inplace=True,
                )
            else:
                raise MetropyError(
                    "Cannot read communes from ADMIN EXPRESS directory "
                    f"`{self.admin_express_directory}`"
                )
        else:
            communes = self.get_ign_data(
                self.api_communes_service_name,
                [
                    "code_insee",
                    "code_insee_du_departement",
                    "code_insee_de_la_region",
                    "nom_officiel",
                ],
                bbox,
            )
            communes.rename(
                columns={
                    "code_insee": "insee_id",
                    "code_insee_du_departement": "departement_id",
                    "code_insee_de_la_region": "region_id",
                    "nom_officiel": "name",
                },
                inplace=True,
            )
        return communes


class IRISStep(IGNStep):
    """Abstract Step to retrieve data from the Contours IRIS database.

    Data is retrieved from local file when the
    [`contours_iris_directory`](parameters.md#igncontours_iris_directory) parameter is defined,
    otherwise data is requested from the WFS API.
    """

    contours_iris_directory = PathParameter(
        "ign.contours_iris_directory",
        check_dir_exists=True,
        description="Path to the directory where the Contours IRIS database is stored.",
        note=(
            "The database can be downloaded from "
            "[cartes.gouv.fr](https://cartes.gouv.fr/rechercher-une-donnee/dataset/IGNF_CONTOURS-IRIS)."
            "The GPKG format is recommended. "
            "The 7z file needs to be extracted."
        ),
    )
    api_iris_service_name = StringParameter(
        "ign.api_iris_service_name",
        default="STATISTICALUNITS.IRIS:contours_iris",
        description="Name of the API service from which to request IRIS data.",
        note=(
            'For faster but less accurate queries, you can use the "STATISTICALUNITS.IRIS.PE" '
            "version."
        ),
    )

    def read_iris(self, bbox: tuple[float, float, float, float] | None = None) -> gpd.GeoDataFrame:
        """Returns a GeoDataFrame of IRIS read from the Contours IRIS database.

        When the `bbox` parameter is specified, only returns IRIS that intersect that bbox.
        """
        if self.contours_iris_directory is not None:
            gpkg_file = find_file("iris.gpkg", self.contours_iris_directory, recursive=True)
            shp_file = find_file("CONTOURS-IRIS.shp", self.contours_iris_directory, recursive=True)
            if bbox is None:
                mask = None
            else:
                # Put the bbox in a gpd.GeoSeries so that CRS mis-match are properly resolved.
                mask = gpd.GeoSeries([box(*bbox)], crs="EPSG:4326")
            if gpkg_file is not None:
                iris = gpd.read_file(
                    gpkg_file,
                    columns=["geometry", "code_insee", "code_iris", "nom_iris"],
                    mask=mask,
                )
                iris.rename(
                    columns={"code_insee": "insee_id", "code_iris": "iris_id", "nom_iris": "name"},
                    inplace=True,
                )
            elif shp_file is not None:
                iris = gpd.read_file(
                    shp_file, columns=["geometry", "INSEE_COM", "CODE_IRIS", "NOM_IRIS"], mask=mask
                )
                iris.rename(
                    columns={"INSEE_COM": "insee_id", "CODE_IRIS": "iris_id", "NOM_IRIS": "name"},
                    inplace=True,
                )
            else:
                raise MetropyError(
                    "Cannot read IRIS from Contours IRIS directory "
                    f"`{self.contours_iris_directory}`"
                )
        else:
            iris = self.get_ign_data(
                self.api_iris_service_name, ["code_insee", "code_iris", "nom_iris"], bbox
            )
            iris.rename(
                columns={"code_insee": "insee_id", "code_iris": "iris_id", "nom_iris": "name"},
                inplace=True,
            )
        return iris


IGN_STEPS = [AdminExpressStep, IRISStep]
