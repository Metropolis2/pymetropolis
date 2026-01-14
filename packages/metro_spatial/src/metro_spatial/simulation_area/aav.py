import zipfile

import geopandas as gpd
from loguru import logger
from metro_common.errors import MetropyError, error_context
from metro_common.io import read_geodataframe
from metro_common.types import Path
from metro_common.utils import tmp_download
from metro_pipeline import Config, ConfigTable, ConfigValue, Step

from metro_spatial import CRS

from .common import buffer_area, geom_as_gdf
from .file import SIMULATION_AREA_FILE

# URL to download the Aire d'attraction des villes shapefiles.
AAV_URL = "https://www.insee.fr/fr/statistiques/fichier/4803954/fonds_aav2020_2024.zip"

AAV_NAME = ConfigValue(
    "simulation_area.aav_name",
    key="aav_name",
    expected_type=str,
    description="Name of the _Aire d'attraction des villes_ to be selected.",
    example="Paris",
    note="The value must appears in the column `libaav20xx` of the `aav_filename` file.",
)

AAV_FILENAME = ConfigValue(
    "simulation_area.aav_filename",
    key="aav_filename",
    expected_type=Path(extensions=[".zip", ".shp"]),
    description="Path to the shapefile of the French's _Aires d'attraction des villes_.",
    example='`"data/aav2020_2024.zip"`',
    note=(
        "When the value is not specified, pymetropolis will attempt to automatically download the "
        "shapefile."
    ),
)

BUFFER = ConfigValue(
    "simulation_area.buffer",
    key="buffer",
    expected_type=float,
    default=0.0,
    description="Distance by which the polygon of the simulation area must be extended or shrinked.",
    example="`500`",
    note=(
        "The value is expressed in the unit of measure of the CRS (usually meter). Positive values "
        "extend the area, while negative values shrink it."
    ),
)

SIMULATION_AREA_FROM_AAV_TABLE = ConfigTable(
    "simulation_area_from_aav",
    "simulation_area_from_aav",
    items=[AAV_NAME, AAV_FILENAME, BUFFER],
    description=(
        "Creates a polygon of the simulation area from a given French _Aire d'attraction des "
        "villes_."
    ),
)

AAV_CONFIG = [SIMULATION_AREA_FROM_AAV_TABLE, AAV_NAME, AAV_FILENAME, BUFFER]


@error_context(msg="Cannot create simulation area from French's Aires d'attraction des villes")
def from_french_aav(config: Config):
    logger.info("Reading polygon from Aires d'attraction des villes")
    if config.get(AAV_FILENAME) is not None:
        # Read the GeoDataFrame from the provided file.
        gdf = read_geodataframe(config.get(AAV_FILENAME))
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
    name = config[AAV_NAME]
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
    aav.to_crs(config[CRS], inplace=True)
    aav0 = aav.iloc[0]
    if aav0[lib_col] != name:
        logger.warning(
            f"No exact match for the AAV name `{name}`, using AAV `{aav0[lib_col]}` instead"
        )
    geom = aav0.geometry
    if config[BUFFER] != 0.0:
        geom = buffer_area(geom, config[BUFFER])
    gdf = geom_as_gdf(geom, config[CRS])
    SIMULATION_AREA_FILE.save(gdf, config)
    return True


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


SIMULATION_AREA_FROM_AAV = Step(
    "simulation-area-from-aav",
    from_french_aav,
    output_files=[SIMULATION_AREA_FILE],
    config_values=[CRS, AAV_NAME, AAV_FILENAME, BUFFER],
)
