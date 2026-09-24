"""Generic import of a network from OpenStreetMap data, using DuckDB.

The OSM file is read with the `ST_ReadOSM` function of DuckDB's spatial extension (parallel scan of
the PBF file) and all the processing (filtering, splitting of ways at intersections, cleaning of
the tags, identification of the node features, construction of the geometries) is done in SQL.
Nothing but the final edges is ever materialized in Python.

Network-specific behaviors are defined by overriding the methods of `OpenStreetMapNetworkImport`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_spatial.osm import keep_join_build_side, open_osm_duckdb, sql_literal

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    import duckdb
    import geopandas as gpd
    import pyproj
    from shapely.geometry import MultiPolygon, Polygon


def sql_list(values: Iterable[str]) -> str:
    """Returns the values as a SQL list of string literals, e.g., `('a', 'b')`."""
    return "({})".format(", ".join(sql_literal(v) for v in values))


def sql_mapping(col: str, mapping: dict[str, int] | dict[str, float]) -> str:
    """Returns a SQL expression mapping the values of a column to numerical values (other values
    are mapped to NULL).
    """
    cases = " ".join(f"WHEN {sql_literal(key)} THEN {value!r}" for key, value in mapping.items())
    return f"CASE {col} {cases} END"


def directed_speed_and_lanes(
    maxspeed: str = "maxspeed",
    maxspeed_forward: str = "maxspeed_forward",
    maxspeed_backward: str = "maxspeed_backward",
) -> str:
    """Returns SQL columns `forward_speed_limit`, `backward_speed_limit`, `forward_lanes` and
    `backward_lanes`, from cleaned (numerical) speed-limit columns and raw `lanes*` columns.

    Tags maxspeed:forward and maxspeed:backward (respectively lanes:forward and lanes:backward) are
    read when available, otherwise tag maxspeed (respectively lanes) is read (the number of lanes
    is divided by two when the way is not oneway).

    The `oneway` column must be defined.
    """
    return f"""
        coalesce({maxspeed_forward}, {maxspeed}) AS forward_speed_limit,
        CASE WHEN NOT oneway THEN coalesce({maxspeed_backward}, {maxspeed}) END
            AS backward_speed_limit,
        CASE
            WHEN oneway
            THEN coalesce(TRY_CAST(lanes_forward AS DOUBLE), TRY_CAST(lanes AS DOUBLE))
            ELSE coalesce(TRY_CAST(lanes_forward AS DOUBLE), TRY_CAST(lanes AS DOUBLE) / 2.0)
        END AS forward_lanes,
        CASE
            WHEN NOT oneway
            THEN coalesce(TRY_CAST(lanes_backward AS DOUBLE), TRY_CAST(lanes AS DOUBLE) / 2.0)
        END AS backward_lanes
    """


def directional_node_features(features: Iterable[str]) -> list[str]:
    """Returns SQL aggregate expressions defining columns `forward_{feature}` and
    `backward_{feature}`, identifying the segments that contain nodes with a particular feature
    (e.g., traffic signals, stop signs), given as the `highway` tag of the nodes.

    Segment is marked as having the feature in the forward direction if:
    - One of the segment's nodes is marked as having the feature in forward direction.
    - The target node of the segment is marked as having the feature (with no direction specified).
    - The segment is "oneway" and one of its nodes is marked as having the feature (with no
      direction specified).

    Segment is marked as having the feature in the backward direction if:
    - One of the segment's nodes is marked as having the feature in backward direction.
    - The source node of the segment is marked as having the feature (with no direction
      specified).

    Segments are in the forward direction of the way so, when traveling backward, the source node
    of the segment is the node which is reached.

    The expressions must be used in `OpenStreetMapNetworkImport.node_features_query`, with
    node columns `highway` and `direction` (see `DIRECTION_NODE_COLUMN`) and way column `oneway`.
    """
    columns = list()
    for feature in features:
        is_feat = f"f.highway = {sql_literal(feature)}"
        no_dir = f"{is_feat} AND f.direction IS NULL"
        columns.append(
            f"""coalesce(
                bool_or({is_feat} AND f.direction IN ('both', 'forward'))
                OR bool_or({no_dir} AND f.node_id = s.target)
                OR (any_value(w.oneway) AND bool_or({no_dir})),
                false
            ) AS forward_{feature}"""
        )
        columns.append(
            f"""coalesce(
                bool_or({is_feat} AND f.direction IN ('both', 'backward'))
                OR bool_or({no_dir} AND f.node_id = s.source),
                false
            ) AS backward_{feature}"""
        )
    return columns


# SQL expression for the direction of the node features (`direction` tag, NULL if invalid).
DIRECTION_NODE_COLUMN = """
    CASE
        WHEN coalesce(
            nullif(tags['traffic_signals:direction'], ''), nullif(tags['direction'], '')
        ) IN ('both', 'forward', 'backward')
        THEN coalesce(nullif(tags['traffic_signals:direction'], ''), nullif(tags['direction'], ''))
    END
"""


class OpenStreetMapNetworkImport:
    """Generic class to import a network from OpenStreetMap data, using DuckDB.

    Parameters
    ----------
    - osm_file: Path to a OSM file (`.osm.pbf` or `.osm`).
    - highway_tags: list of `highway=*` values that define valid ways for the network.
    - crs: projected CRS to be used for geometric operations, must be a valid pyproj CRS.
    - filter_polygon: optional polygon to filter ways, must be in the same CRS.
    - reindex: if True, set edge ids to `1,...,n`, where `n` is the number of edges (default is
      False).

    The processing is done through the following temporary tables:
    - `ways`: valid ways, with columns `osm_id`, `refs` (node ids) and the columns defined by
      `way_columns`.
    - `way_nodes`: one row per (way, node) pair, with columns `osm_id`, `node_id` and `idx`
      (1-based position of the node in the way).
    - `nodes`: nodes of the valid ways, with columns `node_id`, `x`, `y` (coordinates in the
      projected CRS) and the columns defined by `node_columns`.
    - `segments`: ways split at intersections, with columns `osm_id`, `source`, `source_idx`,
      `target`, `target_idx`. Segments are identified by `(osm_id, source_idx)`.
    - `clean_ways`: cleaned way attributes, from `clean_ways_query` (segments of ways not in that
      table are discarded).
    - `segment_features`: segment attributes read from the nodes, from `segment_features_query`.
    """

    def __init__(
        self,
        osm_file: Path,
        highway_tags: list[str],
        crs: pyproj.CRS,
        filter_polygon: Polygon | MultiPolygon | None,
        reindex: bool = False,
    ):
        self.osm_file = osm_file
        self.highway_tags = highway_tags
        self.crs = crs
        self.filter_polygon = filter_polygon
        self.reindex = reindex

    # === Methods to be overridden to define network-specific behaviors ===

    def extra_way_filter(self) -> str:
        """Returns a SQL condition on the `tags` column that valid ways must satisfy."""
        return "true"

    def way_columns(self) -> dict[str, str]:
        """Returns a dictionary `name -> SQL expression` (from the `tags` column) of the relevant
        OpenStreetMap data to extract from a valid way.
        """
        return {
            "edge_type": "tags['highway']",
            "name": (
                "coalesce(nullif(tags['name'], ''), nullif(tags['addr:street'], ''), "
                "nullif(tags['ref'], ''))"
            ),
        }

    def node_columns(self) -> dict[str, str]:
        """Returns a dictionary `name -> SQL expression` (from the `tags` column) of the relevant
        OpenStreetMap data to extract from a node of the network.
        """
        return dict()

    def clean_ways_query(self) -> str:
        """Returns a SQL query returning the cleaned way attributes from the `ways` table.

        Ways not returned by the query are discarded (after the ways are split at
        intersections).
        """
        return "SELECT * EXCLUDE (refs) FROM ways"

    def segment_features_query(self) -> str | None:
        """Returns a SQL query returning additional attributes of the segments read from node data
        (e.g., traffic signals, stop signs), identified by `(osm_id, source_idx)`, or None if
        there is no such attribute.

        See `node_features_query`.
        """
        return None

    def forward_condition(self) -> str:
        """Returns a SQL condition on the segment columns for the segment to be imported in the
        forward direction.
        """
        return "true"

    def backward_condition(self) -> str:
        """Returns a SQL condition on the segment columns for the segment to be imported in the
        backward direction.

        By default, all segments are duplicated (appropriate for pedestrian networks).
        """
        return "true"

    def directed_features(self) -> list[str]:
        """Returns the list of features `feat` whose values depend on the direction of the edge,
        read from the columns `forward_{feat}` and `backward_{feat}`.
        """
        return list()

    def edge_columns(self) -> list[str]:
        """Returns a list of columns to be kept in the final edge DataFrame (in addition to
        `edge_id`, `source`, `target`, `geometry` and `length`).
        """
        return ["edge_type", "name", "original_id"]

    # === Helpers for subclasses ===

    def node_features_query(self, columns: Iterable[str], node_filter: str) -> str:
        """Returns a SQL query computing aggregate columns over the nodes of each segment.

        The aggregate expressions can refer to the node columns (`f.*`, with nodes restricted to
        the ones satisfying `node_filter`), the segment columns (`s.*`) and the cleaned way
        columns (`w.*`). The aggregates are computed over NULL values for the segments with no
        node satisfying the filter.
        """
        return f"""
            WITH featured_nodes AS (
                SELECT wn.osm_id, wn.idx, n.*
                FROM way_nodes AS wn
                JOIN nodes AS n USING (node_id)
                WHERE {node_filter}
            )
            SELECT s.osm_id, s.source_idx, {", ".join(columns)}
            FROM segments AS s
            JOIN clean_ways AS w USING (osm_id)
            LEFT JOIN featured_nodes AS f
                ON f.osm_id = s.osm_id AND f.idx BETWEEN s.source_idx AND s.target_idx
            GROUP BY s.osm_id, s.source_idx
        """

    # === Generic implementation ===

    def run(self) -> gpd.GeoDataFrame:
        """Runs all operations required to import the network and returns a GeoDataFrame of edges
        with their characteristics.
        """
        with open_osm_duckdb(self.osm_file) as (con, pbf_file):
            logger.info("Reading highway ways")
            self.read_ways(con, pbf_file)
            logger.info("Reading highway nodes")
            self.read_nodes(con, pbf_file)
            self.drop_ways_with_missing_nodes(con)
            self.create_way_points(con)
            if self.filter_polygon is not None:
                logger.info("Filtering ways based on area")
                self.filter_ways_by_area(con, self.filter_polygon)
            logger.info("Splitting ways at intersections")
            self.split_ways(con)
            logger.debug("Cleaning way data")
            con.execute(f"CREATE TEMP TABLE clean_ways AS {self.clean_ways_query()}")
            logger.info("Creating edges")
            return self.create_edges(con)

    def read_ways(self, con: duckdb.DuckDBPyConnection, pbf_file: Path):
        """Creates the `ways` and `way_nodes` tables with the valid highway ways.

        A way is valid if:
        - It has a valid highway tag.
        - It has at least two nodes (closed ways, e.g., roundabouts, are valid).
        - It is valid according to the `extra_way_filter` method.
        """
        columns = ",\n".join(f'{expr} AS "{name}"' for name, expr in self.way_columns().items())
        con.execute(
            f"""
            CREATE TEMP TABLE ways AS
            SELECT id AS osm_id, refs, {columns}
            FROM ST_ReadOSM({sql_literal(str(pbf_file))})
            WHERE kind = 'way'
                AND list_contains($highways, tags['highway'])
                AND len(refs) >= 2
                AND ({self.extra_way_filter()})
            """,
            {"highways": self.highway_tags},
        )
        nb_ways = count_rows(con, "ways")
        logger.debug(f"Number of highway ways: {nb_ways:,}")
        if nb_ways == 0:
            raise MetropyError("No valid way in the OSM data")
        con.execute(
            """
            CREATE TEMP TABLE way_nodes AS
            SELECT osm_id, unnest(refs) AS node_id, generate_subscripts(refs, 1) AS idx
            FROM ways
            """
        )

    def read_nodes(self, con: duckdb.DuckDBPyConnection, pbf_file: Path):
        """Creates the `nodes` table with the nodes of the valid ways, their projected coordinates
        and the columns defined by `node_columns`.
        """
        extra_columns = "".join(
            f', {expr} AS "{name}"' for name, expr in self.node_columns().items()
        )
        with keep_join_build_side(con):
            con.execute(
                f"""
                CREATE TEMP TABLE nodes AS
                WITH needed AS (SELECT DISTINCT node_id AS id FROM way_nodes),
                raw AS (
                    SELECT
                        id AS node_id,
                        ST_Transform(ST_Point(lon, lat), 'EPSG:4326', $crs, always_xy := true)
                            AS point
                        {extra_columns}
                    FROM ST_ReadOSM({sql_literal(str(pbf_file))})
                    SEMI JOIN needed USING (id)
                    WHERE kind = 'node'
                )
                SELECT node_id, ST_X(point) AS x, ST_Y(point) AS y, * EXCLUDE (node_id, point)
                FROM raw
                """,
                {"crs": self.crs.to_wkt()},
            )
        logger.debug(f"Number of highway nodes: {count_rows(con, 'nodes'):,}")

    def drop_ways_with_missing_nodes(self, con: duckdb.DuckDBPyConnection):
        """Drops the ways referencing nodes that are not in the OSM file."""
        con.execute(
            """
            CREATE TEMP TABLE invalid_ways AS
            SELECT DISTINCT osm_id FROM way_nodes ANTI JOIN nodes USING (node_id)
            """
        )
        nb_invalid = count_rows(con, "invalid_ways")
        if nb_invalid > 0:
            logger.warning(f"Dropping {nb_invalid:,} ways with nodes missing from the OSM data")
            keep_ways(con, "SELECT osm_id FROM ways ANTI JOIN invalid_ways USING (osm_id)")
            if count_rows(con, "ways") == 0:
                raise MetropyError("No valid way in the OSM data")

    def create_way_points(self, con: duckdb.DuckDBPyConnection):
        """Creates the `way_points` table with the ordered list of (projected) points of each way.

        An ordered aggregate (`list(... ORDER BY idx)`) is much more memory-intensive so the
        points are aggregated unordered, as structs whose first field is the index, then sorted
        per way.
        """
        con.execute(
            """
            CREATE TEMP TABLE way_points AS
            SELECT osm_id, list_transform(list_sort(points), p -> ST_Point(p.x, p.y)) AS points
            FROM (
                SELECT wn.osm_id, list(struct_pack(idx := wn.idx, x := n.x, y := n.y)) AS points
                FROM way_nodes AS wn
                JOIN nodes AS n USING (node_id)
                GROUP BY wn.osm_id
            )
            """
        )

    def filter_ways_by_area(
        self, con: duckdb.DuckDBPyConnection, filter_polygon: Polygon | MultiPolygon
    ):
        """Only keeps the ways intersecting with the filter polygon.

        The filter must be applied before splitting the ways, as the intersection nodes depend on
        the set of ways.
        """
        con.execute(
            "CREATE TEMP TABLE filter_polygon AS SELECT ST_GeomFromWKB($1) AS geom",
            [filter_polygon.wkb],
        )
        keep_ways(
            con,
            """
            SELECT osm_id
            FROM way_points
            WHERE ST_Intersects(ST_MakeLine(points), (SELECT geom FROM filter_polygon))
            """,
        )
        nb_ways = count_rows(con, "ways")
        logger.debug(f"Number of ways in the area: {nb_ways:,}")
        if nb_ways == 0:
            raise MetropyError("The simulation area does not intersect with the OSM data")

    def split_ways(self, con: duckdb.DuckDBPyConnection):
        """Creates the `segments` table by splitting ways at the node where they are intersected
        by another way.

        The intersection nodes are the nodes which are source or target of a way (first or last
        node) or which appears twice in the data (thus representing an intersection between two
        ways, or a way intersecting with itself).

        Segments with the same source and target are dropped.
        """
        con.execute(
            """
            CREATE TEMP TABLE intersection_nodes AS
            SELECT node_id FROM way_nodes GROUP BY node_id HAVING count(*) > 1
            UNION
            SELECT refs[1] FROM ways
            UNION
            SELECT refs[len(refs)] FROM ways
            """
        )
        con.execute(
            """
            CREATE TEMP TABLE segments AS
            WITH main_nodes AS (
                SELECT osm_id, node_id, idx
                FROM way_nodes
                SEMI JOIN intersection_nodes USING (node_id)
            ),
            paired AS (
                SELECT
                    osm_id,
                    lag(node_id) OVER w AS source,
                    lag(idx) OVER w AS source_idx,
                    node_id AS target,
                    idx AS target_idx
                FROM main_nodes
                WINDOW w AS (PARTITION BY osm_id ORDER BY idx)
            )
            SELECT * FROM paired WHERE source IS NOT NULL AND source <> target
            """
        )
        logger.debug(f"Number of segments: {count_rows(con, 'segments'):,}")

    def create_edges(self, con: duckdb.DuckDBPyConnection) -> gpd.GeoDataFrame:
        """Returns a GeoDataFrame of edges, with the segments duplicated in the forward and
        backward directions, as defined by `forward_condition` and `backward_condition`.

        Features defined by `directed_features` are read for the correct direction.

        Edge ids are `{osm_id}` (forward) or `{osm_id}r` (backward), with `-{i}` appended when the
        way is split in multiple segments (`i` being the index of the segment, when sorted by
        source node). If `reindex` is True, edge ids are instead `1,...,n`.
        """
        import geopandas as gpd

        features_query = self.segment_features_query()
        if features_query is not None:
            logger.debug("Identifying edge features from nodes")
            con.execute(f"CREATE TEMP TABLE segment_features AS {features_query}")
            features_join = "LEFT JOIN segment_features AS sf USING (osm_id, source_idx)"
            features_cols = ", sf.* EXCLUDE (osm_id, source_idx)"
        else:
            features_join = ""
            features_cols = ""
        con.execute(
            f"""
            CREATE TEMP TABLE clean_segments AS
            SELECT
                s.* EXCLUDE (target_idx),
                w.* EXCLUDE (osm_id),
                s.osm_id AS original_id
                {features_cols},
                ST_MakeLine(list_slice(p.points, s.source_idx, s.target_idx)) AS geom
            FROM segments AS s
            JOIN clean_ways AS w USING (osm_id)
            JOIN way_points AS p USING (osm_id)
            {features_join}
            """
        )

        directed = set(self.directed_features())

        def columns(direction: str) -> str:
            return ", ".join(
                f'"{direction}_{col}" AS "{col}"' if col in directed else f'"{col}"'
                for col in self.edge_columns()
            )

        order = "osm_id, backward, source, source_idx"
        bwd_symbol = "CASE WHEN backward THEN 'r' ELSE '' END"
        if self.reindex:
            edge_id = f"row_number() OVER (ORDER BY {order})"
        else:
            edge_id = f"""
                CASE
                    WHEN sum(CASE WHEN backward THEN 0 ELSE 1 END) OVER (PARTITION BY osm_id) > 1
                    THEN osm_id::VARCHAR || {bwd_symbol} || '-'
                        || (row_number() OVER (
                            PARTITION BY osm_id, backward ORDER BY source, source_idx
                        ) - 1)::VARCHAR
                    ELSE osm_id::VARCHAR || {bwd_symbol}
                END
            """
        # OSM ids are cast to unsigned integers, like `source` and `target`.
        replace = (
            "REPLACE (original_id::UBIGINT AS original_id)"
            if "original_id" in self.edge_columns()
            else ""
        )
        logger.debug("Duplicating two-way edges")
        result = con.execute(
            f"""
            WITH edges AS (
                SELECT
                    osm_id,
                    source_idx,
                    false AS backward,
                    source,
                    target,
                    {columns("forward")},
                    geom
                FROM clean_segments
                WHERE {self.forward_condition()}
                UNION ALL
                SELECT
                    osm_id,
                    source_idx,
                    true AS backward,
                    target AS source,
                    source AS target,
                    {columns("backward")},
                    ST_Reverse(geom) AS geom
                FROM clean_segments
                WHERE {self.backward_condition()}
            )
            SELECT
                {edge_id} AS edge_id,
                source::UBIGINT AS source,
                target::UBIGINT AS target,
                * EXCLUDE (osm_id, source_idx, backward, source, target, geom) {replace},
                ST_AsWKB(geom) AS geometry,
                ST_Length(geom) AS length
            FROM edges
            ORDER BY {order}
            """
        ).to_arrow_table()
        logger.debug(f"Number of edges: {result.num_rows:,}")
        geometry = gpd.GeoSeries.from_wkb(result.column("geometry").to_numpy(), crs=self.crs)
        df = result.drop_columns(["geometry", "length"]).to_pandas()
        gdf = gpd.GeoDataFrame(df, geometry=geometry)
        gdf["length"] = result.column("length").to_numpy()
        return gdf


def keep_ways(con: duckdb.DuckDBPyConnection, query: str):
    """Restricts the way tables to the ways whose id is returned by `query`."""
    con.execute(f"CREATE OR REPLACE TEMP TABLE kept_ways AS {query}")
    for table in ("ways", "way_nodes", "way_points"):
        if has_table(con, table):
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE {table} AS "
                f"FROM {table} SEMI JOIN kept_ways USING (osm_id)"
            )


def has_table(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name = $1", [table]
    ).fetchone()
    assert row is not None
    return row[0] > 0


def count_rows(con: duckdb.DuckDBPyConnection, table: str) -> int:
    row = con.execute(f"SELECT count(*) FROM {table}").fetchone()
    assert row is not None
    return row[0]
