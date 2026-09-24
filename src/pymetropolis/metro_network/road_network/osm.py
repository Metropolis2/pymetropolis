"""Import of the road network from OpenStreetMap data, using DuckDB."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_network.osm import (
    DIRECTION_NODE_COLUMN,
    OpenStreetMapNetworkImport,
    directed_speed_and_lanes,
    directional_node_features,
    sql_list,
    sql_literal,
)
from pymetropolis.metro_pipeline.parameters import BoolParameter, FloatParameter, ListParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_pipeline.types import String
from pymetropolis.metro_spatial import GeoStep, OSMStep
from pymetropolis.metro_spatial.simulation_area.file import SimulationAreaFile

from .files import RoadEdgesRawFile

if TYPE_CHECKING:
    from pathlib import Path

    import pyproj
    from shapely.geometry import MultiPolygon, Polygon

# Dictionary for special `maxspeed` values.
SPEED_DICT = {"walk": 8, "FR:walk": 20, "FR:urban": 50, "FR:rural": 80}

# Conversion miles to kilometers.
M_TO_KM = 1.609344

# Directed features of the ways.
FEATURES = ("speed_limit", "lanes", "give_way", "stop", "traffic_signals")

# Node features (`highway=*` tag of the nodes) which are identified on the edges.
NODE_FEATURES = ("give_way", "stop", "traffic_signals")


def speed_expr(col: str) -> str:
    """Returns a SQL expression to read the speed limit (in km/h) from a `maxspeed` tag.

    Special values are handled by `SPEED_DICT`.
    Values in mph, like "30 mph", are converted to km/h.
    """
    special = " ".join(
        f"WHEN {sql_literal(key)} THEN {float(value)}" for key, value in SPEED_DICT.items()
    )
    return f"""coalesce(
        TRY_CAST(nullif(regexp_extract({col}, '([0-9]+) mph', 1), '') AS DOUBLE) * {M_TO_KM},
        CASE {col} {special} ELSE TRY_CAST({col} AS DOUBLE) END
    )"""


class OSMRoadNetworkImport(OpenStreetMapNetworkImport):
    def __init__(
        self,
        osm_file: Path,
        highway_tags: list[str],
        crs: pyproj.CRS,
        filter_polygon: Polygon | MultiPolygon | None,
        allowed_access_tags: list[str],
        reindex: bool = False,
    ):
        super().__init__(
            osm_file=osm_file,
            highway_tags=highway_tags,
            crs=crs,
            filter_polygon=filter_polygon,
            reindex=reindex,
        )
        self.allowed_access_tags = allowed_access_tags

    def extra_way_filter(self) -> str:
        """Returns True if the candidate way has valid road access."""
        if not self.allowed_access_tags:
            return "tags['access'] IS NULL"
        return f"tags['access'] IS NULL OR tags['access'] IN {sql_list(self.allowed_access_tags)}"

    def way_columns(self) -> dict[str, str]:
        return {
            **super().way_columns(),
            "toll": "coalesce(tags['toll'] = 'yes', false)",
            "roundabout": "coalesce(tags['junction'] = 'roundabout', false)",
            "oneway": "coalesce(tags['oneway'] = 'yes', false)",
            "maxspeed": "tags['maxspeed']",
            "maxspeed_forward": "tags['maxspeed:forward']",
            "maxspeed_backward": "tags['maxspeed:backward']",
            "lanes": "tags['lanes']",
            "lanes_forward": "tags['lanes:forward']",
            "lanes_backward": "tags['lanes:backward']",
        }

    def clean_ways_query(self) -> str:
        """Returns the cleaned way data.

        - Classify roundabouts as oneway roads.
        - Clean speedlimit from maxspeed tag.
        - Clean number of lanes from lanes tag.
        """
        return f"""
            WITH oneway_ways AS (
                SELECT * REPLACE (oneway OR roundabout AS oneway) FROM ways
            ),
            speeds AS (
                SELECT
                    *,
                    {speed_expr("maxspeed")} AS maxspeed_clean,
                    {speed_expr("maxspeed_forward")} AS maxspeed_forward_clean,
                    {speed_expr("maxspeed_backward")} AS maxspeed_backward_clean
                FROM oneway_ways
            )
            SELECT
                osm_id,
                edge_type,
                name,
                toll,
                roundabout,
                oneway,
                {
            directed_speed_and_lanes(
                "maxspeed_clean", "maxspeed_forward_clean", "maxspeed_backward_clean"
            )
        }
            FROM speeds
        """

    def node_columns(self) -> dict[str, str]:
        return {"highway": "tags['highway']", "direction": DIRECTION_NODE_COLUMN}

    def segment_features_query(self) -> str:
        """Returns edges with informations on give-way signs, stop signs and traffic signals, read
        from node data.
        """
        return self.node_features_query(
            directional_node_features(NODE_FEATURES), f"n.highway IN {sql_list(NODE_FEATURES)}"
        )

    def backward_condition(self) -> str:
        """Only not oneway edges are duplicated."""
        return "NOT oneway"

    def directed_features(self) -> list[str]:
        return list(FEATURES)

    def edge_columns(self) -> list[str]:
        return [*super().edge_columns(), "toll", "roundabout", "oneway", *FEATURES]


class OpenStreetMapRoadImportStep(GeoStep, OSMStep):
    """Imports a road network from OpenStreetMap data.

    Edges of the road network are read from the OpenStreetMap ways with tag
    [highway:*](https://wiki.openstreetmap.org/wiki/Key:highway).

    The [`osm_road_import.highways`](parameters.md#osm_road_importhighways) parameter is used to
    define the `highway` values which are part of the road network. For example, values `"motorway"`
    and `"motorway_link"` should usually be included, while value `"footway"` should be excluded as
    it does not represent ways accessible by road vehicles.

    In addition to the `highways` parameter, this step requires both the
    [`osm_file`](parameters.md#osm_file) and [`crs`](parameters.md#crs) parameters to be set.

    For example, to import the major roads of Paris, you can use:

    ```toml
    osm_file = "path/to/paris.osm.pbf"
    crs = "epsg:2154"

    [osm_road_import]
    highways = [
      "motorway",
      "motorway_link",
      "trunk",
      "trunk_link",
      "primary",
      "primary_link",
      "secondary",
      "secondary_link",
    ]
    ```

    An OpenStreetMap way is selected as valid road-network edge if it satisfies all the conditions
    below:

    - Tag `highway` matches one of the value given in
      [`highways`](parameters.md#osm_road_importhighways) parameter.
    - The way has no tag `access` or tag `access` matches one of the value given in
      [`allowed_access`](parameters.md#osm_road_importallowed_access).
    - The way has at least two nodes (closed ways, like roundabouts, are valid).
    - All the way's nodes are in the OpenStreetMap data.
    - The way intersects with the simulation area (if
      [`simulation_area_filter`](parameters.md#osm_road_importsimulation_area_filter) is `true`).

    By default, the allowed tags for access are `yes`, `permissive` (non-public roads with allowed
    access), and `destination` (allowed for local traffic only).

    When filtering with the simulation area, the
    [`simulation_area_buffer`](parameters.md#osm_road_importsimulation_area_buffer) parameter can be
    used to extend or shrink the area by a given distance.
    This can be useful to include edges _outside_ the area that might be used when traveling between
    two points _inside_ the area.

    Various processing operations are done to clean the edges:

    - Duplicate `oneway=false` ways in two opposing edges.
    - Split ways at nodes where another way is intersecting.

    Edges attributes are defined as follows:

    - `edge_id`: OSM id of the way, with "r" appended if the edge is going backward, with "-[idx]"
      appended if the edge is split in multiple segments (with `idx` the segment index).
    - `source`: OSM id of the source node.
    - `target`: OSM id of the target node.
    - `original_id`: OSM id of the way (note that values are generally not unique).
    - `length`: computed as geometric operation on the ways' LineString, after conversion to the
      simulation CRS.
    - `edge_type`: `highway` tag value.
    - `name`: `name` tag value if any, otherwise `addr:street` tag value if any, otherwise `ref` tag
      value if any.
    - `speed_limit`: read from `maxspeed`, `maxspeed:forward`, or `maxspeed:backward` tag, as
      appropriate.
    - `lanes`: read from `lanes`, `lanes:forward`, or `lanes:backward` tag, as appropriate.
    - `oneway`: `oneway` tag value.
    - `roundabout`: `True` if `junction=roundabout`.
    - `toll`: `True` if `toll=yes`.
    - `give_way`: way has a node with `highway=give_way`, in the correct direction.
    - `stop`: way has a node with `highway=stop`, in the correct direction.
    - `traffic_signals`: way has a node with `highway=traffic_signals`, in the correct direction.

    If the [`reindex`](parameters.md#osm_road_importreindex) parameter is set to `true`, the
    `edge_id` values are instead numerical values running from 1 to the number of edges.
    """

    highways = ListParameter(
        "osm_road_import.highways",
        inner=String(),
        min_length=1,
        description="List of `highway=*` OpenStreetMap tags to be considered as valid road ways.",
        example='`["motorway", "motorway_link", "trunk", "trunk_link", "primary", "primary_link"]`',
        note=(
            "A list of highway tags with description is available on the "
            "[OpenStreetMap wiki](https://wiki.openstreetmap.org/wiki/Key:highway)."
        ),
    )
    allowed_access = ListParameter(
        "osm_road_import.allowed_access",
        inner=String(),
        default=["yes", "permissive", "destination"],
        description=(
            "List of `access=*` OpenStreetMap tags defining ways accessible to road vehicles."
        ),
        note=(
            "Any way with an `access` value that is not in the given list will be considered as "
            "not accessible to road vehicles and thus will not be imported. "
            "A list of access tags with description is available on the "
            "[OpenStreetMap wiki](https://wiki.openstreetmap.org/wiki/Key:access)."
        ),
    )
    reindex = BoolParameter(
        "osm_road_import.reindex",
        default=False,
        description=(
            "If `true`, the edges are re-index from 1 to n. If `false`, edge ids match the "
            "OpenStreetMap way ids."
        ),
    )
    simulation_area_filter = BoolParameter(
        "osm_road_import.simulation_area_filter",
        default=True,
        description=(
            "Whether the road network must be restricted to the edges within the simulation area."
        ),
    )
    simulation_area_buffer = FloatParameter(
        "osm_road_import.simulation_area_buffer",
        default=0.0,
        description=(
            "Distance by which the polygon of the simulation area must be extended or shrinked "
            "when importing the road network."
        ),
        note=(
            "The value is expressed in the unit of measure of the CRS (usually meter). "
            "Positive values extend the area, while negative values shrink it."
        ),
    )

    input_files = {
        "simulation_area": InputFile(
            SimulationAreaFile,
            when=lambda inst: inst.simulation_area_filter,
            when_doc="if `simulation_area_filter` is set to `true`",
        )
    }
    output_files = {"raw_edges": RoadEdgesRawFile}

    def is_defined(self) -> bool:
        return self.crs is not None and self.osm_file is not None and self.highways is not None

    def run(self):
        assert self.crs is not None
        assert self.osm_file is not None
        assert self.highways is not None
        assert self.allowed_access is not None
        assert self.reindex is not None

        if self.simulation_area_filter:
            filter_polygon: Polygon | MultiPolygon = self.input["simulation_area"].get_area()  # ty: ignore[unresolved-attribute]
            filter_polygon = filter_polygon.buffer(self.simulation_area_buffer)
        else:
            filter_polygon = None
        importer = OSMRoadNetworkImport(
            osm_file=self.osm_file,
            highway_tags=self.highways,
            crs=self.crs,
            filter_polygon=filter_polygon,
            allowed_access_tags=self.allowed_access,
            reindex=self.reindex,
        )
        edges = importer.run()
        self.output["raw_edges"].write(edges)
