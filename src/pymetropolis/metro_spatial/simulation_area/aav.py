import zipfile

import geopandas as gpd
from loguru import logger

from pymetropolis.metro_common.errors import MetropyError, error_context
from pymetropolis.metro_common.io import read_geodataframe
from pymetropolis.metro_common.utils import tmp_download
from pymetropolis.metro_pipeline.parameters import (
    FloatParameter,
    PathParameter,
    StringParameter,
)
from pymetropolis.metro_spatial import GeoStep

from .common import buffer_area, geom_as_gdf
from .file import SimulationAreaFile

# URL to download the Aire d'attraction des villes shapefiles.
AAV_URL = "https://www.insee.fr/fr/statistiques/fichier/4803954/fonds_aav2020_2024.zip"


@error_context(msg=f"Cannot download AAV database from url `{AAV_URL}`")
def get_aav_from_url():
    with tmp_download(AAV_URL) as fn:
        with zipfile.ZipFile(fn) as z:
            valid_files = [
                name for name in z.namelist() if name.startswith("aav20") and name.endswith(".zip")
            ]
            assert len(valid_files) == 1
            gdf = gpd.read_file(z.open(valid_files[0]), engine="pyogrio")
            return gdf


class SimulationAreaFromAAVStep(GeoStep):
    """Creates the simulation area from the boundaries of a Frech metropolitan area.

    A French metropolitan area (*aire d'attraction d'une ville*) is a type of statistical area
    defined by the French national statistics office INSEE.
    It is defined by considering the commuting patterns between cities, making it well adapted to
    define areas for transport simulations.

    The database for these *aires d'attraction des villes* is publicly available on the
    [INSEE website](https://www.insee.fr/fr/information/4803954).
    Pymetropolis can automatically download the database and read the polygon of an area if you set
    the `aav_name` parameter to one of the existing area.
    The areas' name is usually the name of the biggest city in the area.

    ```toml
    [simulation_area]
    aav_name = "Paris"
    ```

    If the automatic download does not work, you can download the file locally and tell Pymetropolis
    to use that version:

    - Go to the INSEE page of the *aires d'attraction des villes* database:
      [https://www.insee.fr/fr/information/4803954](https://www.insee.fr/fr/information/4803954)
    - Download the zip file "Fonds de cartes des aires d'attraction des villes 2020 au 1er janvier
      2024"
    - Unzip the file. You will get two zip files representing shapefiles: `aav2020_2024.zip`
      (polygons of the areas) and `com_aav2020_2024.zip` (polygons of the municipalities). Only the
      former is needed.
    - In the section `[simulation_area]` of the configuration, add the lines
      `aav_filename = "path/to/aav2020_2024.zip"` and `aav_name = "YOUR_AAV_NAME"`.

    ```toml
    [simulation_area]
    aav_name = "Paris"
    aav_filename = "path/to/aav2020_2024.zip"
    ```
    """

    aav_name = StringParameter(
        "simulation_area.aav_name",
        description="Name of the _Aire d'attraction des villes_ to be selected.",
        example="Paris",
        note="The value must appears in the column `libaav20xx` of the `aav_filename` file.",
    )
    aav_filename = PathParameter(
        "simulation_area.aav_filename",
        check_file_exists=True,
        extensions=[".zip", ".shp"],
        description="Path to the shapefile of the French's _Aires d'attraction des villes_.",
        example='`"data/aav2020_2024.zip"`',
        note=(
            "When the value is not specified, pymetropolis will attempt to automatically download the "
            "shapefile."
        ),
    )
    buffer = FloatParameter(
        "simulation_area.buffer",
        default=0.0,
        description="Distance by which the polygon of the simulation area must be extended or shrinked.",
        note=(
            "The value is expressed in the unit of measure of the CRS (usually meter). Positive values "
            "extend the area, while negative values shrink it."
        ),
    )
    output_files = {"simulation_area": SimulationAreaFile}

    def is_defined(self) -> bool:
        return self.crs is not None and self.aav_name is not None

    def run(self):
        logger.info("Reading polygon from Aires d'attraction des villes")
        if self.aav_filename is not None:
            # Read the GeoDataFrame from the provided file.
            gdf = read_geodataframe(self.aav_filename)
        else:
            # Try to download and read the AAV filename.
            gdf = get_aav_from_url()
        # Find column name.
        lib_col = None
        for col in gdf.columns:
            if col.startswith("libaav20"):
                lib_col = col
                break
        else:
            raise MetropyError("Cannot find `libaav20xx` column")
        name = self.aav_name
        # Try first to find an exact match, then a match by starting substring, then a match by
        # containing string.
        mask = gdf[lib_col] == name
        if not mask.any():
            # Does not start with `{name}`, followed by a letter (case insensitive).
            mask = gdf[lib_col].str.contains(f"^{name}(?![a-z])", regex=True, case=False)
            if not mask.any():
                # Does not contain `{name}` (case insensitive).
                mask = gdf[lib_col].str.contains(name, case=False)
                if not mask.any():
                    raise MetropyError(f"No AAV with name `{name}`")
        if mask.sum() > 1:
            raise MetropyError(f"Multiple AAVs match the name `{name}`")
        # At this point, the mask has only 1 valid entry.
        aav = gdf.loc[mask].copy()
        aav.to_crs(self.crs, inplace=True)
        aav0 = aav.iloc[0]
        if aav0[lib_col] != name:
            logger.warning(
                f"No exact match for the AAV name `{name}`, using AAV `{aav0[lib_col]}` instead"
            )
        geom = aav0.geometry
        if self.buffer != 0.0:
            geom = buffer_area(geom, self.buffer)
        gdf = geom_as_gdf(geom, self.crs)
        self.output["simulation_area"].write(gdf)
