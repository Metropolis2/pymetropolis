from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from loguru import logger

from pymetropolis.metro_network.osm import OpenStreetMapNetworkImport
from pymetropolis.metro_pipeline.parameters import BoolParameter, FloatParameter, ListParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_pipeline.types import String
from pymetropolis.metro_spatial import GeoStep, OSMStep
from pymetropolis.metro_spatial.simulation_area.file import SimulationAreaFile

from .files import BicycleEdgesRawFile

if TYPE_CHECKING:
    import polars as pl
    from osmium.osm import Node, Way
    from shapely.geometry import MultiPolygon, Polygon

# Directed features of the ways.
FEATURES = ("speed_limit", "lanes", "give_way", "stop", "traffic_signals", "type")


class OSMBicycleNetworkImport(OpenStreetMapNetworkImport):
    def extra_way_filter(self, way: Way) -> bool:
        """Returns True if the candidate way has valid bicycle access."""
        return "bicycle" not in way.tags or way.tags["bicycle"] not in ("no", "private")

    def way_data(self, way: Way) -> dict[str, Any]:
        """Returns a dictionary with the relevant OpenStreetMap data to extract from a valid way."""

        data = super().way_data(way)
        data.update(
            {
                "bicycle": way.tags.get("bicycle"),
                "segregated": way.tags.get("segregated"),
                "cycleway": way.tags.get("cycleway"),
                "cycleway:left": way.tags.get("cycleway:left"),
                "cycleway:left:oneway": way.tags.get("cycleway:left:oneway"),
                "cycleway:right": way.tags.get("cycleway:right"),
                "cycleway:right:oneway": way.tags.get("cycleway:right:oneway"),
                "cycleway:both": way.tags.get("cycleway:both"),
                "roundabout": way.tags.get("junction") == "roundabout",
                "oneway": way.tags.get("oneway") == "yes",
                "oneway:bicycle": way.tags.get("oneway:bicycle"),
                "maxspeed": way.tags.get("maxspeed"),
                "maxspeed:forward": way.tags.get("maxspeed:forward"),
                "maxspeed:backward": way.tags.get("maxspeed:backward"),
                "lanes": way.tags.get("lanes"),
                "lanes:forward": way.tags.get("lanes:forward"),
                "lanes:backward": way.tags.get("lanes:backward"),
                "surface": way.tags.get("surface"),
                "cycleway:surface": way.tags.get("cycleway:surface"),
                "smoothness": way.tags.get("smoothness"),
                "tracktype": way.tags.get("tracktype"),
            }
        )
        return data

    def way_data_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        """Returns a dictionary representing the Polars schema for the DataFrame constructed from
        `way_data`."""
        import polars as pl

        schema = super().way_data_schema()
        schema.update(
            {
                "bicycle": pl.String,
                "segregated": pl.String,
                "cycleway": pl.String,
                "cycleway:left": pl.String,
                "cycleway:left:oneway": pl.String,
                "cycleway:right": pl.String,
                "cycleway:right:oneway": pl.String,
                "cycleway:both": pl.String,
                "roundabout": pl.Boolean,
                "oneway": pl.Boolean,
                "oneway:bicycle": pl.String,
                "maxspeed": pl.String,
                "maxspeed:forward": pl.String,
                "maxspeed:backward": pl.String,
                "lanes": pl.String,
                "lanes:forward": pl.String,
                "lanes:backward": pl.String,
                "surface": pl.String,
                "cycleway:surface": pl.String,
                "smoothness": pl.String,
                "tracktype": pl.String,
            }
        )
        return schema

    def clean_way_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Returns a cleaned version of data collected from `way_data`.

        - Classify roundabouts as oneway roads.
        - Clean speedlimit from maxspeed tag.
        - Clean number of lanes from lanes tag.
        - Drop self ways.
        """
        import polars as pl

        # Roundabouts are oneway.
        df = df.with_columns(oneway=pl.col("oneway").or_(pl.col("roundabout")))
        df = df.with_columns(
            category=pl.when(edge_type="cycleway")
            .then(pl.lit("cycleway"))
            .when(pl.col("edge_type").is_in(("crossing", "footway", "path", "track")))
            .then(pl.lit("shared"))
            .otherwise(pl.lit("road")),
            bicycle=pl.when(
                pl.col("bicycle").is_in(
                    ("yes", "designated", "use_sidepath", "discouraged", "dismount", "permissive")
                )
            ).then("bicycle"),
            segregated=pl.when(pl.col("segregated").is_in(("yes", "no"))).then("segregated"),
            cycleway=pl.when(
                pl.col("cycleway")
                .replace({"traffic_island": "crossing"})
                .is_in(
                    (
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
                    )
                )
            ).then("cycleway"),
            cycleway_left=pl.when(
                pl.col("cycleway:left").is_in(
                    (
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
                    )
                )
            ).then("cycleway:left"),
            cycleway_left_oneway=pl.col("cycleway:left:oneway").replace({"opposite": "-1"}),
            cycleway_right=pl.when(
                pl.col("cycleway:right").is_in(
                    (
                        "no",
                        "lane",
                        "separate",
                        "share_busway",
                        "shared_lane",
                        "track",
                        "opposite",
                        "opposite_lane",
                    )
                )
            ).then("cycleway:right"),
            cycleway_right_oneway="cycleway:right:oneway",
            cycleway_both=pl.when(
                pl.col("cycleway:both").is_in(
                    ("no", "separate", "lane", "shared_lane", "share_busway", "track")
                )
            ).then("cycleway:both"),
            oneway="oneway",
            oneway_bicycle=pl.when(pl.col("oneway:bicycle").is_in(("no", "yes"))).then(
                "oneway:bicycle"
            ),
        )
        definitions = pl.read_csv(Path(__file__).parent / "cycleways.csv")
        df = df.join(
            definitions,
            on=[
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
            ],
            how="left",
        )
        # Find maximum speed if available.
        df = df.with_columns(
            pl.col(col).cast(pl.Float64, strict=False)
            for col in ("maxspeed", "maxspeed:forward", "maxspeed:backward")
        ).with_columns(
            # Read tags maxspeed:forward and maxspeed:backward when available, otherwise read tag
            # maxspeed.
            forward_speed_limit=pl.col("maxspeed:forward").fill_null(pl.col("maxspeed")),
            backward_speed_limit=pl.when("oneway")
            .then(pl.lit(None))
            .otherwise(pl.col("maxspeed:backward").fill_null(pl.col("maxspeed"))),
        )
        # Find number of lanes if available.
        # Read tags lanes:forward and lanes:backward when available, otherwise read tag lanes.
        df = df.with_columns(
            pl.col("lanes").cast(pl.Float64, strict=False),
            pl.col("lanes:forward").cast(pl.Float64, strict=False),
            pl.col("lanes:backward").cast(pl.Float64, strict=False),
        ).with_columns(
            forward_lanes=pl.when("oneway")
            .then(pl.col("lanes:forward").fill_null(pl.col("lanes")))
            .otherwise(pl.col("lanes:forward").fill_null(pl.col("lanes") / 2.0)),
            backward_lanes=pl.when("oneway")
            .then(pl.lit(None))
            .otherwise(pl.col("lanes:backward").fill_null(pl.col("lanes") / 2.0)),
        )
        # Mark crossings.
        df = df.with_columns(
            forward_type=pl.when(edge_type="crossing")
            .then(pl.lit("crossing"))
            .otherwise("forward_type"),
            backward_type=pl.when(edge_type="crossing")
            .then(pl.lit("crossing"))
            .otherwise("backward_type"),
        )
        # Drop rows with source == target.
        df = df.filter(pl.col("source") != pl.col("target"))
        # Drop undefined edges.
        df = df.filter(pl.col("forward_type").is_not_null() | pl.col("backward_type").is_not_null())
        # Compute road quality.
        df = df.with_columns(
            surface=pl.col("cycleway:surface").fill_null(pl.col("surface"))
        ).with_columns(
            quality=pl.min_horizontal(
                pl.col("tracktype").replace_strict(
                    {"grade1": 7, "grade2": 5, "grade3": 4, "grade4": 3, "grade5": 2}, default=None
                ),
                pl.col("smoothness")
                .replace_strict(
                    {
                        "excellent": 10,
                        "good": 8,
                        "intermediate": 7,
                        "bad": 5,
                        "very_bad": 3,
                        "horrible": 2,
                        "very_horrible": 1,
                        "impassable": 0,
                    },
                    default=None,
                )
                .fill_null(8),
                pl.col("surface")
                .replace_strict(
                    {
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
                    },
                    default=None,
                )
                .fill_null(8),
            ).cast(pl.UInt8)
        )
        df: pl.DataFrame = df.select(
            "osm_id",
            "source",
            "target",
            "edge_type",
            "name",
            "oneway",
            "roundabout",
            "forward_speed_limit",
            "backward_speed_limit",
            "forward_lanes",
            "backward_lanes",
            "forward_type",
            "backward_type",
            "quality",
            "nodes",
        )
        return df

    def node_data(self, node: Node) -> dict[str, Any]:
        """Returns a dictionary with the relevant OpenStreetMap data to extract from a node of the
        network.
        """
        data = super().node_data(node)
        data.update(
            {
                "highway": node.tags.get("highway"),
                "direction": node.tags.get("traffic_signals:direction")
                or node.tags.get("direction"),
                "bump": node.tags.get("traffic_calming") in ("bump", "hump"),
            }
        )
        return data

    def node_data_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        """Returns a dictionary representing the Polars schema for the DataFrame constructed from
        `node_data`.
        """
        import polars as pl

        schema = super().node_data_schema()
        schema.update({"highway": pl.String, "direction": pl.String, "bump": pl.Boolean})
        return schema

    def add_node_features_to_edges(self, edges: pl.DataFrame, nodes: pl.DataFrame) -> pl.DataFrame:
        """Returns edges with informations on give-way signs, stop signs and traffic signals, read
        from node data.
        """
        import polars as pl

        nodes = nodes.with_columns(
            pl.when(pl.col("direction").is_in(("both", "forward", "backward")))
            .then("direction")
            .otherwise(pl.lit(None))
        )
        for feature in ("give_way", "stop", "traffic_signals"):
            edges = self.identify_edge_features(edges, nodes, feature)

        bump_nodes = nodes.filter("bump")["osm_id"].to_list()
        edges = edges.with_columns(
            has_bump=pl.col("nodes").list.eval(pl.element().is_in(bump_nodes)).list.any()
        )
        return edges

    def identify_edge_features(
        self, edges: pl.DataFrame, nodes: pl.DataFrame, feature: str
    ) -> pl.DataFrame:
        """Identifies the edges that contain nodes with a particular feature (e.g., traffic signals,
        stop signs).

        Edges is marked as having the feature in the forward direction if:
        - One of the edge's nodes is marked as having the feature in forward direction.
        - The target node of the edge is marked as having the feature (with no direction specified).
        - The edge is "oneway" and one of its nodes is marked as having the feature (with no
          direction specified).

        Edges is marked as having the feature in the backward direction if:
        - One of the edge's nodes is marked as having the feature in backward direction.
        - The source node of the edge is marked as having the feature (with no direction specified).
        """
        import polars as pl

        logger.debug(f"Identifying edges with {feature} nodes")
        featured_nodes = nodes.filter(pl.col("highway") == feature)
        fwd_node_ids = featured_nodes.filter(pl.col("direction").is_in(("both", "forward")))[
            "osm_id"
        ]
        bwd_node_ids = featured_nodes.filter(pl.col("direction").is_in(("both", "backward")))[
            "osm_id"
        ]
        no_dir_node_ids = featured_nodes.filter(pl.col("direction").is_null())["osm_id"]
        edges = edges.with_columns(
            (
                pl.col("nodes").list.eval(pl.element().is_in(fwd_node_ids)).list.any()
                | pl.col("target").is_in(no_dir_node_ids)
                | pl.col("oneway").and_(
                    pl.col("nodes").list.eval(pl.element().is_in(no_dir_node_ids)).list.any()
                )
            ).alias(f"forward_{feature}"),
            (
                pl.col("nodes").list.eval(pl.element().is_in(bwd_node_ids)).list.any()
                | pl.col("source").is_in(no_dir_node_ids)
            ).alias(f"backward_{feature}"),
        )
        return edges

    def duplicate_edges(self, edges: pl.DataFrame) -> pl.DataFrame:
        """Duplicates edges in the forward and backward direction.

        Only not oneway edges are duplicated.

        Features (speedlimit, lanes, etc.) are read for the correct direction.
        """
        import polars as pl

        # Duplicate two-way ways.
        lf = edges.lazy()
        forward_edges = lf.filter(pl.col("forward_type").is_not_null()).with_columns(
            *(pl.col(f"forward_{feat}").alias(feat) for feat in FEATURES), backward=False
        )
        backward_edges = lf.filter(pl.col("backward_type").is_not_null()).with_columns(
            pl.col("source").alias("target"),
            pl.col("target").alias("source"),
            pl.col("nodes").list.reverse(),
            *(pl.col(f"backward_{feat}").alias(feat) for feat in FEATURES),
            backward=True,
        )
        return pl.concat((forward_edges, backward_edges), how="vertical").collect()  # ty: ignore[invalid-return-type]

    def edge_columns(self) -> list[str]:
        """Returns a list of columns to be kept in the final edge DataFrame."""
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
    - The way's geometry is a valid LineString.
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
    simulation_area_filter = BoolParameter(
        "osm_bicycle_import.simulation_area_filter",
        default=True,
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
        )
        edges = importer.run()
        self.output["raw_edges"].write(edges)
