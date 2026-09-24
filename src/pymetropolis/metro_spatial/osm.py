from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loguru import logger

from pymetropolis.metro_pipeline.parameters import PathParameter
from pymetropolis.metro_pipeline.steps import Step

if TYPE_CHECKING:
    from collections.abc import Iterator

    import duckdb
    import geopandas as gpd


class OSMStep(Step):
    osm_file = PathParameter(
        "osm_file",
        check_file_exists=True,
        extensions=[".pbf", ".osm"],
        description=(
            "Path to the OpenStreetMap file (`.osm` or `.osm.pbf`) with data for the simulation "
            "area."
        ),
        example='`"data/osm/france-250101.osm.pbf"`',
        note=(
            "You can download extract of OpenStreetMap data for any region in the world through "
            "the Geofabrik website. You can also download data directly from the OSM website, "
            "using the “Export” button, although it is limited to small areas."
        ),
        shared=True,
    )


def sql_literal(value: str) -> str:
    """Returns the value as a SQL string literal."""
    return "'{}'".format(value.replace("'", "''"))


@contextmanager
def open_osm_duckdb(osm_file: Path) -> Iterator[tuple[duckdb.DuckDBPyConnection, Path]]:
    """Context manager yielding an in-memory DuckDB connection (with the spatial extension loaded)
    and the path to a PBF version of the OSM file, to be read with `ST_ReadOSM`.

    `ST_ReadOSM` can only read PBF files so other formats (e.g., `.osm` XML files) are converted
    with osmium first (in a temporary directory which is removed afterward).
    """
    import tempfile

    import duckdb

    with tempfile.TemporaryDirectory(prefix="pymetropolis_osm_") as tmp:
        tmp_dir = Path(tmp)
        if osm_file.suffix == ".pbf":
            pbf_file = osm_file
        else:
            import osmium

            logger.info("Converting OSM file to PBF format")
            pbf_file = tmp_dir / "data.osm.pbf"
            with osmium.SimpleWriter(str(pbf_file)) as writer:
                for obj in osmium.FileProcessor(str(osm_file)):
                    writer.add(obj)
        con = duckdb.connect()
        try:
            try:
                con.load_extension("spatial")
            except duckdb.Error:
                con.install_extension("spatial")
                con.load_extension("spatial")
            # Row order is never relied upon (explicit ORDER BY must be used), this reduces memory
            # usage.
            con.execute("SET preserve_insertion_order = false")
            # Spill to the temporary directory.
            con.execute("SET temp_directory = $1", [str(tmp_dir / "duckdb")])
            yield con, pbf_file
        finally:
            con.close()


@contextmanager
def keep_join_build_side(con: duckdb.DuckDBPyConnection) -> Iterator[None]:
    """Context manager to run queries joining `ST_ReadOSM` with a table of ids.

    `ST_ReadOSM` reports a cardinality of 1 so, by default, DuckDB builds the hash table of the
    join on the scan side (i.e., on all the elements of the file) instead of the (much smaller)
    table of ids. Disabling the optimizer keeps the build side as written (right side), which
    greatly reduces memory usage.
    """
    con.execute("SET disabled_optimizers = 'build_side_probe_side'")
    try:
        yield
    finally:
        con.execute("RESET disabled_optimizers")


def read_osm_areas(
    osm_file: Path, condition: str, params: dict[str, Any], columns: dict[str, str]
) -> gpd.GeoDataFrame:
    """Returns a GeoDataFrame (in EPSG:4326) with the OpenStreetMap areas whose tags satisfy the
    SQL `condition` (on the `tags` column, with parameters `params`).

    Areas are read, as with osmium, from the relations of type `multipolygon` or `boundary` and
    from the closed ways. The polygons of the relations are built from the member ways (the rings
    and their nesting are identified from the linework). Invalid polygons (e.g., with overlapping
    inner rings, which are not valid OSM multipolygons) are made valid. Areas with member ways or
    nodes missing from the OSM data are skipped.

    The GeoDataFrame has columns `osm_id`, `kind` (`"way"` or `"relation"`), `geometry` and the
    columns defined by `columns` (dictionary `name -> SQL expression`, from the `tags` column).
    """
    import geopandas as gpd

    extra_columns = "".join(f', {expr} AS "{name}"' for name, expr in columns.items())
    with open_osm_duckdb(osm_file) as (con, pbf_file):
        osm = f"ST_ReadOSM({sql_literal(str(pbf_file))})"
        con.execute(
            f"""
            CREATE TEMP TABLE areas AS
            SELECT kind::VARCHAR AS kind, id, refs, ref_types {extra_columns}
            FROM {osm}
            WHERE kind IN ('way', 'relation')
                AND ({condition})
                AND (
                    (kind = 'way' AND len(refs) >= 4 AND refs[1] = refs[len(refs)])
                    OR (kind = 'relation' AND tags['type'] IN ('multipolygon', 'boundary'))
                )
            """,
            params,
        )
        # Ways forming each area (a closed way forms its own area).
        con.execute(
            """
            CREATE TEMP TABLE members AS
            SELECT DISTINCT kind, id, way_id
            FROM (
                SELECT kind, id, unnest(refs) AS way_id, unnest(ref_types) AS ref_type
                FROM areas
                WHERE kind = 'relation'
            )
            WHERE ref_type = 'way'
            UNION ALL
            SELECT kind, id, id AS way_id FROM areas WHERE kind = 'way'
            """
        )
        with keep_join_build_side(con):
            con.execute(
                f"""
                CREATE TEMP TABLE member_ways AS
                SELECT id AS way_id, refs
                FROM {osm}
                SEMI JOIN (SELECT DISTINCT way_id AS id FROM members) USING (id)
                WHERE kind = 'way'
                """
            )
            con.execute(
                f"""
                CREATE TEMP TABLE member_nodes AS
                SELECT id AS node_id, lon, lat
                FROM {osm}
                SEMI JOIN (SELECT DISTINCT unnest(refs) AS id FROM member_ways) USING (id)
                WHERE kind = 'node'
                """
            )
        # Lines of the member ways (ways with missing nodes are discarded). The points are
        # aggregated unordered, as structs whose first field is the index, then sorted (an ordered
        # aggregate is much more memory-intensive).
        con.execute(
            """
            CREATE TEMP TABLE lines AS
            SELECT way_id, ST_MakeLine(list_transform(list_sort(points), p -> ST_Point(p.x, p.y)))
                AS line
            FROM (
                SELECT
                    w.way_id,
                    list(struct_pack(idx := w.idx, x := n.lon, y := n.lat)) AS points,
                    bool_and(n.node_id IS NOT NULL) AS complete
                FROM (
                    SELECT way_id, unnest(refs) AS node_id, generate_subscripts(refs, 1) AS idx
                    FROM member_ways
                ) AS w
                LEFT JOIN member_nodes AS n USING (node_id)
                GROUP BY w.way_id
            )
            WHERE complete
            """
        )
        result = con.execute(
            """
            SELECT
                a.id AS osm_id,
                a.kind,
                any_value(COLUMNS(a.* EXCLUDE (kind, id, refs, ref_types))),
                ST_AsWKB(
                    ST_MakeValid(
                        ST_BuildArea(ST_Collect(list(l.line) FILTER (l.line IS NOT NULL)))
                    )
                ) AS geometry,
                bool_or(l.line IS NULL) AS incomplete
            FROM areas AS a
            JOIN members AS m USING (kind, id)
            LEFT JOIN lines AS l USING (way_id)
            GROUP BY a.kind, a.id
            ORDER BY a.kind, a.id
            """
        ).to_arrow_table()
    df = result.drop_columns("geometry").to_pandas()
    incomplete = df.pop("incomplete").to_numpy()
    if incomplete.any():
        logger.warning(
            f"Skipping {incomplete.sum():,} areas with members missing from the OpenStreetMap data"
        )
    geometry = gpd.GeoSeries.from_wkb(result.column("geometry").to_numpy(), crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    gdf = gdf.loc[~incomplete & ~gdf.geometry.is_empty].reset_index(drop=True)
    logger.debug(f"Number of areas: {len(gdf):,}")
    return gdf
