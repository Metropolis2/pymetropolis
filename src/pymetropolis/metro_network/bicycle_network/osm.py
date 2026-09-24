"""Import of the bicycle network from OpenStreetMap data, using DuckDB."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pymetropolis.metro_network.osm import (
    DIRECTION_NODE_COLUMN,
    OpenStreetMapNetworkImport,
    directed_speed_and_lanes,
    directional_node_features,
    sql_list,
    sql_literal,
    sql_mapping,
)
from pymetropolis.metro_pipeline.parameters import BoolParameter, FloatParameter, ListParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_pipeline.types import String
from pymetropolis.metro_spatial import GeoStep, OSMStep
from pymetropolis.metro_spatial.simulation_area.file import SimulationAreaFile

from .files import BicycleEdgesRawFile

if TYPE_CHECKING:
    from shapely.geometry import MultiPolygon, Polygon

# Directed features of the ways.
FEATURES = ("speed_limit", "lanes", "give_way", "stop", "traffic_signals", "type")

# Node features (`highway=*` tag of the nodes) which are identified on the edges.
NODE_FEATURES = ("give_way", "stop", "traffic_signals")

# Valid values of the tags used to identify the cycleway type (other values are set to NULL).
VALID_TAG_VALUES = {
    "bicycle": ("yes", "designated", "use_sidepath", "discouraged", "dismount", "permissive"),
    "segregated": ("yes", "no"),
    "cycleway": (
        "no",
        "crossing",
        "lane",
        "opposite",
        "shared_lane",
        "separate",
        "share_busway",
        "opposite_lane",
        "track",
        "link",
    ),
    "cycleway_left": (
        "no",
        "lane",
        "separate",
        "opposite_lane",
        "shared_lane",
        "share_busway",
        "opposite",
        "track",
        "opposite_share_busway",
        "opposite_track",
    ),
    "cycleway_right": (
        "no",
        "lane",
        "separate",
        "share_busway",
        "shared_lane",
        "track",
        "opposite",
        "opposite_lane",
    ),
    "cycleway_both": ("no", "separate", "lane", "shared_lane", "share_busway", "track"),
    "oneway_bicycle": ("no", "yes"),
}

# Columns of `cycleways.csv` used to identify the cycleway type of the ways.
DEFINITION_KEYS = (
    "category",
    "bicycle",
    "segregated",
    "cycleway",
    "cycleway_left",
    "cycleway_left_oneway",
    "cycleway_right",
    "cycleway_right_oneway",
    "cycleway_both",
    "oneway",
    "oneway_bicycle",
)

TRACKTYPE_QUALITY = {"grade1": 7, "grade2": 5, "grade3": 4, "grade4": 3, "grade5": 2}

SMOOTHNESS_QUALITY = {
    "excellent": 10,
    "good": 8,
    "intermediate": 7,
    "bad": 5,
    "very_bad": 3,
    "horrible": 2,
    "very_horrible": 1,
    "impassable": 0,
}

SURFACE_QUALITY = {
    "asphalt": 10,
    "paved": 8,
    "concrete": 8,
    "concrete:plates": 8,
    "concrete:lanes": 7,
    "paving_stones": 6,
    "compacted": 6,
    "wood": 6,
    "metal": 6,
    "unpaved": 5,
    "grass_paver": 4,
    "ground": 4,
    "sett": 4,
    "fine_gravel": 4,
    "cobblestone": 4,
    "unhewn_cobblestone": 4,
    "gravel": 4,
    "earth": 3,
    "dirt": 3,
    "mud": 3,
    "woodchips": 2,
    "grass": 2,
    "pebblestone": 2,
    "sand": 1,
}


def valid_value(col: str, key: str | None = None) -> str:
    """Returns a SQL expression equal to the column value if it is valid, NULL otherwise."""
    return f"CASE WHEN {col} IN {sql_list(VALID_TAG_VALUES[key or col])} THEN {col} END"


class OSMBicycleNetworkImport(OpenStreetMapNetworkImport):
    def extra_way_filter(self) -> str:
        """Ways with no bicycle access are excluded."""
        return "coalesce(tags['bicycle'] NOT IN ('no', 'private'), true)"

    def way_columns(self) -> dict[str, str]:
        return {
            **super().way_columns(),
            "bicycle_tag": "tags['bicycle']",
            "segregated_tag": "tags['segregated']",
            "cycleway_tag": "tags['cycleway']",
            "cycleway_left_tag": "tags['cycleway:left']",
            "cycleway_left_oneway_tag": "tags['cycleway:left:oneway']",
            "cycleway_right_tag": "tags['cycleway:right']",
            "cycleway_right_oneway_tag": "tags['cycleway:right:oneway']",
            "cycleway_both_tag": "tags['cycleway:both']",
            "roundabout": "coalesce(tags['junction'] = 'roundabout', false)",
            "oneway": "coalesce(tags['oneway'] = 'yes', false)",
            "oneway_bicycle_tag": "tags['oneway:bicycle']",
            "maxspeed": "tags['maxspeed']",
            "maxspeed_forward": "tags['maxspeed:forward']",
            "maxspeed_backward": "tags['maxspeed:backward']",
            "lanes": "tags['lanes']",
            "lanes_forward": "tags['lanes:forward']",
            "lanes_backward": "tags['lanes:backward']",
            "surface": "tags['surface']",
            "cycleway_surface": "tags['cycleway:surface']",
            "smoothness": "tags['smoothness']",
            "tracktype": "tags['tracktype']",
        }

    def clean_ways_query(self) -> str:
        """Returns the cleaned way data.

        - Classify roundabouts as oneway roads.
        - Identify the cycleway type in both directions, from the definitions in `cycleways.csv`
          (ways with no valid type in any direction are discarded).
        - Clean speedlimit from maxspeed tag.
        - Clean number of lanes from lanes tag.
        - Compute road quality from the tracktype, smoothness and surface tags.
        """
        definitions = Path(__file__).parent / "cycleways.csv"
        # Missing values match with missing values.
        join_condition = " AND ".join(
            f"k.{key} IS NOT DISTINCT FROM d.{key}" for key in DEFINITION_KEYS
        )
        cycleway = "CASE WHEN cycleway_tag = 'traffic_island' THEN 'crossing' ELSE cycleway_tag END"
        return f"""
            WITH definitions AS (
                SELECT * REPLACE (oneway::BOOLEAN AS oneway)
                FROM read_csv({sql_literal(str(definitions))}, header = true, all_varchar = true)
            ),
            keys AS (
                SELECT
                    * REPLACE (oneway OR roundabout AS oneway),
                    CASE
                        WHEN edge_type = 'cycleway' THEN 'cycleway'
                        WHEN edge_type IN ('crossing', 'footway', 'path', 'track') THEN 'shared'
                        ELSE 'road'
                    END AS category,
                    {valid_value("bicycle_tag", "bicycle")} AS bicycle,
                    {valid_value("segregated_tag", "segregated")} AS segregated,
                    CASE
                        WHEN {cycleway} IN {sql_list(VALID_TAG_VALUES["cycleway"])}
                        THEN cycleway_tag
                    END AS cycleway,
                    {valid_value("cycleway_left_tag", "cycleway_left")} AS cycleway_left,
                    CASE
                        WHEN cycleway_left_oneway_tag = 'opposite' THEN '-1'
                        ELSE cycleway_left_oneway_tag
                    END AS cycleway_left_oneway,
                    {valid_value("cycleway_right_tag", "cycleway_right")} AS cycleway_right,
                    cycleway_right_oneway_tag AS cycleway_right_oneway,
                    {valid_value("cycleway_both_tag", "cycleway_both")} AS cycleway_both,
                    {valid_value("oneway_bicycle_tag", "oneway_bicycle")} AS oneway_bicycle
                FROM ways
            ),
            typed AS (
                SELECT
                    k.*,
                    CASE WHEN k.edge_type = 'crossing' THEN 'crossing' ELSE d.forward_type END
                        AS forward_type,
                    CASE WHEN k.edge_type = 'crossing' THEN 'crossing' ELSE d.backward_type END
                        AS backward_type,
                    TRY_CAST(k.maxspeed AS DOUBLE) AS maxspeed_clean,
                    TRY_CAST(k.maxspeed_forward AS DOUBLE) AS maxspeed_forward_clean,
                    TRY_CAST(k.maxspeed_backward AS DOUBLE) AS maxspeed_backward_clean,
                    coalesce(k.cycleway_surface, k.surface) AS surface_clean
                FROM keys AS k
                LEFT JOIN definitions AS d ON {join_condition}
            )
            SELECT
                osm_id,
                edge_type,
                name,
                oneway,
                roundabout,
                {
            directed_speed_and_lanes(
                "maxspeed_clean", "maxspeed_forward_clean", "maxspeed_backward_clean"
            )
        },
                forward_type,
                backward_type,
                least(
                    {sql_mapping("tracktype", TRACKTYPE_QUALITY)},
                    coalesce({sql_mapping("smoothness", SMOOTHNESS_QUALITY)}, 8),
                    coalesce({sql_mapping("surface_clean", SURFACE_QUALITY)}, 8)
                )::UTINYINT AS quality
            FROM typed
            WHERE forward_type IS NOT NULL OR backward_type IS NOT NULL
        """

    def node_columns(self) -> dict[str, str]:
        return {
            "highway": "tags['highway']",
            "direction": DIRECTION_NODE_COLUMN,
            "bump": "coalesce(tags['traffic_calming'] IN ('bump', 'hump'), false)",
        }

    def segment_features_query(self) -> str:
        """Returns edges with informations on give-way signs, stop signs, traffic signals and
        bumps, read from node data.
        """
        return self.node_features_query(
            [
                *directional_node_features(NODE_FEATURES),
                "coalesce(bool_or(f.bump), false) AS has_bump",
            ],
            f"n.highway IN {sql_list(NODE_FEATURES)} OR n.bump",
        )

    def forward_condition(self) -> str:
        return "forward_type IS NOT NULL"

    def backward_condition(self) -> str:
        return "backward_type IS NOT NULL"

    def directed_features(self) -> list[str]:
        return list(FEATURES)

    def edge_columns(self) -> list[str]:
        return [*super().edge_columns(), "quality", "has_bump", *FEATURES]


class OpenStreetMapBicycleImportStep(GeoStep, OSMStep):
    """Imports a bicycle network from OpenStreetMap data.

    Edges of the bicycle network are read from the OpenStreetMap ways with tag
    [highway:*](https://wiki.openstreetmap.org/wiki/Key:highway).

    The [`osm_bicycle_import.highways`](parameters.md#osm_bicycle_importhighways) parameter is used
    to define the `highway` values which are part of the bicycle network. For example, values
    `"cycleway"` and `"path"` should usually be included, while value `"motorway"` should be
    excluded as it does not represent ways accessible by bicycles.

    In addition to the `highways` parameter, this step requires both the
    [`osm_file`](parameters.md#osm_file) and [`crs`](parameters.md#crs) parameters to be set.

    For example, to import the cycleways in Paris, you can use:

    ```toml
    osm_file = "path/to/paris.osm.pbf"
    crs = "epsg:2154"

    [osm_bicycle_import]
    highways = [
      "tertiary",
      "tertiary_link",
      "residential",
      "cycleway",
      "path",
    ]
    ```

    An OpenStreetMap way is selected as valid cycleway edge if it satisfies all the conditions
    below:

    - Tag `highway` matches one of the value given in
      [`highways`](parameters.md#osm_bicycle_importhighways) parameter.
    - The way has no tag `bicycle` or tag `bicycle` is not `"no"`.
    - The way has at least two nodes (closed ways are valid).
    - All the way's nodes are in the OpenStreetMap data.
    - The way intersects with the simulation area (if
      [`simulation_area_filter`](parameters.md#osm_bicycle_importsimulation_area_filter) is `true`).

    When filtering with the simulation area, the
    [`simulation_area_buffer`](parameters.md#osm_bicycle_importsimulation_area_buffer) parameter can
    be used to extend or shrink the area by a given distance.
    This can be useful to include edges _outside_ the area that might be used when traveling between
    two points _inside_ the area.

    Various processing operations are done to clean the edges:

    - Duplicate `oneway=false` ways in two opposing edges.
    - Split ways at nodes where another way is intersecting.

    Edges attributes are defined as follows: TODO

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

    If the [`reindex`](parameters.md#osm_bicycle_importreindex) parameter is set to `true`, the
    `edge_id` values are instead numerical values running from 1 to the number of edges.
    """

    highways = ListParameter(
        "osm_bicycle_import.highways",
        inner=String(),
        min_length=1,
        description=(
            "List of `highway=*` OpenStreetMap tags to be considered as valid bicycle ways."
        ),
        example='`["tertiary", "tertiary_link", "residential", "cycleway", "path"]`',
        note=(
            "A list of highway tags with description is available on the "
            "[OpenStreetMap wiki](https://wiki.openstreetmap.org/wiki/Key:highway)."
        ),
    )
    reindex = BoolParameter(
        "osm_bicycle_import.reindex",
        default=False,
        description=(
            "If `true`, the edges are re-index from 1 to n. If `false`, edge ids match the "
            "OpenStreetMap way ids."
        ),
    )
    simulation_area_filter = BoolParameter(
        "osm_bicycle_import.simulation_area_filter",
        default=False,
        description=(
            "Whether the bicycle network must be restricted to the edges within the simulation "
            "area."
        ),
    )
    simulation_area_buffer = FloatParameter(
        "osm_bicycle_import.simulation_area_buffer",
        default=0.0,
        description=(
            "Distance by which the polygon of the simulation area must be extended or shrinked "
            "when importing the bicycle network."
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
    output_files = {"raw_edges": BicycleEdgesRawFile}

    def is_defined(self) -> bool:
        return self.crs is not None and self.osm_file is not None and self.highways is not None

    def run(self):
        assert self.crs is not None
        assert self.osm_file is not None
        assert self.highways is not None
        assert self.reindex is not None

        if self.simulation_area_filter:
            filter_polygon: Polygon | MultiPolygon = self.input["simulation_area"].get_area()  # ty: ignore[unresolved-attribute]
            filter_polygon = filter_polygon.buffer(self.simulation_area_buffer)
        else:
            filter_polygon = None
        importer = OSMBicycleNetworkImport(
            osm_file=self.osm_file,
            highway_tags=self.highways,
            crs=self.crs,
            filter_polygon=filter_polygon,
            reindex=self.reindex,
        )
        edges = importer.run()
        self.output["raw_edges"].write(edges)
