from pathlib import Path
from typing import Any

import polars as pl
import pyproj
from loguru import logger
from osmium.osm import Node, Way
from shapely.geometry import Polygon

from pymetropolis.metro_network.osm import OpenStreetMapNetworkImport
from pymetropolis.metro_pipeline.parameters import ListParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_pipeline.types import String
from pymetropolis.metro_spatial import GeoStep, OSMStep
from pymetropolis.metro_spatial.simulation_area.file import SimulationAreaFile

from .files import RoadEdgesRawFile

# Dictionary for special `maxspeed` values.
SPEED_DICT = {"walk": 8, "FR:walk": 20, "FR:urban": 50, "FR:rural": 80}

# Conversion miles to kilometers.
M_TO_KM = 1.609344

# Directed features of the ways.
FEATURES = ("speed_limit", "lanes", "give_way", "stop", "traffic_signals")


class OSMRoadNetworkImport(OpenStreetMapNetworkImport):
    def __init__(
        self,
        osm_file: Path,
        highway_tags: list[str],
        crs: pyproj.CRS,
        filter_polygon: Polygon | None,
        allowed_access_tags: list[str],
    ):
        super().__init__(
            osm_file=osm_file, highway_tags=highway_tags, crs=crs, filter_polygon=filter_polygon
        )
        self.allowed_access_tags = allowed_access_tags

    def extra_way_filter(self, way: Way) -> bool:
        """Returns True if the candidate way has valid road access."""
        return "access" not in way.tags or way.tags["access"] in self.allowed_access_tags

    def way_data(self, way: Way) -> dict[str, Any]:
        """Returns a dictionary with the relevant OpenStreetMap data to extract from a valid way."""
        data = super().way_data(way)
        data.update(
            {
                "toll": way.tags.get("toll") == "yes",
                "roundabout": way.tags.get("junction") == "roundabout",
                "oneway": way.tags.get("oneway") == "yes",
                "maxspeed": way.tags.get("maxspeed"),
                "maxspeed:forward": way.tags.get("maxspeed:forward"),
                "maxspeed:backward": way.tags.get("maxspeed:backward"),
                "lanes": way.tags.get("lanes"),
                "lanes:forward": way.tags.get("lanes:forward"),
                "lanes:backward": way.tags.get("lanes:backward"),
            }
        )
        return data

    def way_data_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        """Returns a dictionary representing the Polars schema for the DataFrame constructed from
        `way_data`."""
        schema = super().way_data_schema()
        schema.update(
            {
                "toll": pl.Boolean,
                "roundabout": pl.Boolean,
                "oneway": pl.Boolean,
                "maxspeed": pl.String,
                "maxspeed:forward": pl.String,
                "maxspeed:backward": pl.String,
                "lanes": pl.String,
                "lanes:forward": pl.String,
                "lanes:backward": pl.String,
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
        # Roundabouts are oneway.
        df = df.with_columns(oneway=pl.col("oneway").or_(pl.col("roundabout")))
        # Find maximum speed if available.
        # Special values are handled by `SPEED_DICT`.
        # Values in mph, like "30 mph", are converted to km/h.
        df = df.with_columns(
            pl.col(col)
            .str.extract("([0-9]+) mph")
            .cast(pl.Float64, strict=False)
            .mul(M_TO_KM)
            .fill_null(pl.col(col).replace(SPEED_DICT).cast(pl.Float64, strict=False))
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
        # Drop rows with source == target.
        df = df.filter(pl.col("source") != pl.col("target"))
        df: pl.DataFrame = df.select(
            "osm_id",
            "source",
            "target",
            "road_type",
            "name",
            "toll",
            "roundabout",
            "oneway",
            "forward_speed_limit",
            "backward_speed_limit",
            "forward_lanes",
            "backward_lanes",
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
            }
        )
        return data

    def node_data_schema(self) -> dict[str, pl.DataType | type[pl.DataType]]:
        """Returns a dictionary representing the Polars schema for the DataFrame constructed from
        `node_data`.
        """
        schema = super().node_data_schema()
        schema.update({"highway": pl.String, "direction": pl.String})
        return schema

    def add_node_features_to_edges(self, edges: pl.DataFrame, nodes: pl.DataFrame) -> pl.DataFrame:
        """Returns edges with informations on give-way signs, stop signs and traffic signals, read
        from node data.
        """
        nodes = nodes.with_columns(
            pl.when(pl.col("direction").is_in(("both", "forward", "backward")))
            .then("direction")
            .otherwise(pl.lit(None))
        )
        for feature in ("give_way", "stop", "traffic_signals"):
            edges = self.identify_edge_features(edges, nodes, feature)
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
        # Duplicate two-way ways.
        lf = edges.lazy()
        forward_edges = lf.with_columns(
            *(pl.col(f"forward_{feat}").alias(feat) for feat in FEATURES), backward=False
        )
        backward_edges = lf.filter(pl.col("oneway").not_()).with_columns(
            pl.col("source").alias("target"),
            pl.col("target").alias("source"),
            pl.col("nodes").list.reverse(),
            *(pl.col(f"backward_{feat}").alias(feat) for feat in FEATURES),
            backward=True,
        )
        return pl.concat((forward_edges, backward_edges), how="vertical").collect()  # ty: ignore[invalid-return-type]

    def edge_columns(self) -> list[str]:
        """Returns a list of columns to be kept in the final edge DataFrame."""
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
    - The way's geometry is a valid LineString.
    - The way intersects with the simulation area (if the
      [SimulationAreaFile](files.md#simulationareafile)) exists).

    By default, the allowed tags for access are `yes`, `permissive` (non-public roads with allowed
    access), and `destination` (allowed for local traffic only).

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
    - `road_type`: `highway` tag value.
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

    input_files = {"simulation_area": InputFile(SimulationAreaFile, optional=True)}
    output_files = {"raw_edges": RoadEdgesRawFile}

    def is_defined(self) -> bool:
        return self.crs is not None and self.osm_file is not None and self.highways is not None

    def run(self):
        importer = OSMRoadNetworkImport(
            osm_file=self.osm_file,
            highway_tags=self.highways,
            crs=self.crs,
            filter_polygon=self.input["simulation_area"].get_area_opt(),  # ty: ignore[unresolved-attribute]
            allowed_access_tags=self.allowed_access,
        )
        edges = importer.run()
        self.output["raw_edges"].write(edges)
