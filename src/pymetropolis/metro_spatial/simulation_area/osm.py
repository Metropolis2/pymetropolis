from __future__ import annotations

from typing import TYPE_CHECKING, Any

from loguru import logger

from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_pipeline.parameters import CustomParameter, FloatParameter, IntParameter
from pymetropolis.metro_spatial import GeoStep, OSMStep
from pymetropolis.metro_spatial.osm import read_osm_areas

from .common import buffer_area, geom_as_gdf
from .file import SimulationAreaFile

if TYPE_CHECKING:
    from pathlib import Path

    import geopandas as gpd


def name_or_names_validator(value: Any) -> str | list[str]:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        if all(isinstance(v, str) for v in value):
            return value
        else:
            raise MetropyError(f"List items are not all string: `{value}`")
    raise MetropyError(f"Invalid value (expected string or list of strings): `{value}`")


class SimulationAreaFromOSMStep(GeoStep, OSMStep):
    """Creates the simulation area by reading administrative boundaries from OpenStreetMap data.

    Administrative boundaries of various subdivisions are specified directly on OpenStreetMap (e.g.,
    states, counties, municipalities), with the tags
    [`admin_level=*`](https://wiki.openstreetmap.org/wiki/Key:admin%20level) and
    [`boundary=administrative`](https://wiki.openstreetmap.org/wiki/Tag:boundary%3Dadministrative).
    The OpenStreetMap wiki has a
    [table](https://wiki.openstreetmap.org/wiki/Tag:boundary%3Dadministrative#Table_:_Admin_level_for_all_countries)
    indicating the meaning of each `admin_level` value by country.
    For example, `admin_level=6` represents counties in the U.S. and *départements* in France.

    You can use this Step to create the simulation area by reading one or more administrative
    boundaries from OpenStreetMap data.
    First, you need to set the `osm_file` value to the path to the OpenStreetMap file.
    In the `[simulation_area]` section, the `osm_admin_level` value represents the `admin_level`
    value to be used as filter and the `osm_name` value is a list of the subdivisions names to be
    selected.

    For example, to get the polygon of Madrid, you can use:

    ```toml
    osm_file = "path/to/spain.osm.pbf"

    [simulation_area]
    osm_admin_level = 8
    osm_name = ["Madrid"]
    ```

    Or, to get the polygon of Paris and the surrounding departments, you can use:

    ```toml
    osm_file = "path/to/france.osm.pbf"

    [simulation_area]
    osm_admin_level = 6
    osm_name = ["Paris", "Hauts-de-Seine", "Seine-Saint-Denis", "Val-de-Marne"]
    ```
    """

    osm_name = CustomParameter(
        "simulation_area.osm_name",
        validator=name_or_names_validator,
        validator_description="string or list of strings",
        description=(
            "List of subdivision names to be considered when reading administrative boundaries."
        ),
        example='`"Madrid"`',
        note=(
            "The values are compared with the `name=*` tag of the OpenStreetMap features. "
            "Be careful, the name can sometimes be in the local language."
        ),
    )
    osm_admin_level = IntParameter(
        "simulation_area.osm_admin_level",
        description="Administrative level to be considered when reading administrative boundaries.",
        note=(
            "See https://wiki.openstreetmap.org/wiki/Tag:boundary%3Dadministrative"
            "#Table_:_Admin_level_for_all_countries "
            "for a table with the meaning of all possible value for each country."
        ),
    )
    buffer = FloatParameter(
        "simulation_area.buffer",
        default=0.0,
        description=(
            "Distance by which the polygon of the simulation area must be extended or shrunk."
        ),
        note=(
            "The value is expressed in the unit of measure of the CRS (usually meter). "
            "Positive values extend the area, while negative values shrink it."
        ),
    )
    output_files = {"simulation_area": SimulationAreaFile}

    def is_defined(self) -> bool:
        return (
            self.crs is not None
            and self.osm_file is not None
            and self.osm_name is not None
            and self.osm_admin_level is not None
        )

    def run(self):
        assert self.osm_file is not None
        assert self.osm_admin_level is not None
        assert self.osm_name is not None

        names = [self.osm_name] if isinstance(self.osm_name, str) else self.osm_name
        if len(names) == 0:
            raise MetropyError("You must provide at least one name to be selected")
        logger.debug("Reading areas from OSM file")
        gdf = read_admin_areas(self.osm_file, self.osm_admin_level, names)
        if gdf.empty:
            raise MetropyError(
                "The OpenStreetMap data does not contain any relation with "
                f"`admin_level={self.osm_admin_level}` and `name` in `{names}`"
            )
        missing_names = set(names).difference(set(gdf["name"]))
        if missing_names:
            logger.warning(f"No relation was found for the following names: {missing_names}")
        logger.debug("Converting to required CRS")
        gdf.to_crs(self.crs, inplace=True)
        geom = gdf.union_all()
        if self.buffer is not None and self.buffer != 0.0:
            geom = buffer_area(geom, self.buffer)
        gdf = geom_as_gdf(geom, self.crs)
        self.output["simulation_area"].write(gdf)


def read_admin_areas(osm_file: Path, admin_level: int, names: list[str]) -> gpd.GeoDataFrame:
    """Returns a GeoDataFrame (in EPSG:4326) with the name and polygon of the OpenStreetMap areas
    with the given `admin_level` and `name` tags (see `read_osm_areas`).
    """
    gdf = read_osm_areas(
        osm_file,
        "tags['admin_level'] = $level AND list_contains($names, tags['name'])",
        {"level": str(admin_level), "names": names},
        {"name": "tags['name']"},
    )
    return (
        gdf.sort_values(["name", "kind", "osm_id"])
        .loc[:, ["name", "geometry"]]
        .reset_index(drop=True)
    )
