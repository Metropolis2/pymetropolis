from __future__ import annotations

import zipfile
from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_pipeline.parameters import DateParameter, ListParameter
from pymetropolis.metro_pipeline.steps import Step
from pymetropolis.metro_pipeline.types import PathType
from pymetropolis.metro_spatial import GeoStep

from .files import PublicTransitRoutesFile, PublicTransitStopsFile

if TYPE_CHECKING:
    from datetime import date
    from pathlib import Path

    import polars as pl


class GTFSStep(Step):
    gtfs_files = ListParameter(
        "gtfs.files",
        inner=PathType(check_file_exists=True),
        description="List of GTFS files that form the public-transit network.",
        example='`["data/gtfs/madrid-gtfs.zip"]`',
    )
    gtfs_date = DateParameter(
        "gtfs.date",
        description="Date to be considered for the public-transit network.",
        note="Ensure that the GTFS file(s) have active services for this date.",
    )


class ReadPublicTransitNetworkStep(GTFSStep, GeoStep):
    """Reads the input GTFS file(s) and extracts all stops and routes.

    If [`gtfs.date`](parameters.md#gtfsdate) is specified, only stops and routes with at least one
    event on the given date are extracted.
    """

    output_files = {"stops": PublicTransitStopsFile, "routes": PublicTransitRoutesFile}

    def is_defined(self):
        return self.gtfs_files is not None

    def run(self):
        import geopandas as gpd
        import polars as pl

        assert self.gtfs_files is not None

        stops_dfs = list()
        routes_dfs = list()
        for gtfs_file in self.gtfs_files:
            stops, routes = read_gtfs_stops_and_routes(gtfs_file, self.gtfs_date)
            stops_dfs.append(stops)
            routes_dfs.append(routes)
        stops = pl.concat(stops_dfs, how="diagonal_relaxed")
        routes = pl.concat(routes_dfs, how="diagonal_relaxed")
        for name, df, id_col in (("stops", stops, "stop_id"), ("routes", routes, "route_id")):
            n = df[id_col].is_duplicated().sum()
            if n:
                logger.warning(f"Discarding {n:,} {name} with an id already used in another GTFS")
        stops = stops.unique(subset="stop_id", keep="first", maintain_order=True)
        routes = routes.unique(subset="route_id", keep="first", maintain_order=True)
        logger.info(f"Found {len(stops):,} stops and {len(routes):,} routes")

        stops_gdf = gpd.GeoDataFrame(
            stops.drop("stop_lon", "stop_lat").to_pandas(),
            geometry=gpd.points_from_xy(stops["stop_lon"], stops["stop_lat"], crs="EPSG:4326"),
        ).to_crs(self.crs)
        self.output["stops"].write(stops_gdf)
        self.output["routes"].write(routes)


# Location types, from the GTFS specification.
LOCATION_TYPES = {
    "0": "platform",
    "1": "station",
    "2": "entrance",
    "3": "generic",
    "4": "boarding_area",
}


def read_gtfs_table(z: zipfile.ZipFile, name: str, columns: list[str] | None = None):
    """Reads a table of a GTFS zipfile, with all columns as strings.

    Returns `None` if the table does not exist.
    Columns of `columns` that are missing in the table are created with null values.
    """
    import polars as pl

    filename = next((f for f in z.namelist() if f.split("/")[-1] == f"{name}.txt"), None)
    if filename is None:
        return None
    df = pl.read_csv(z.read(filename), infer_schema=False)
    # Remove whitespaces (and BOM) in column names.
    df = df.rename(lambda col: col.strip().lstrip("﻿"))
    if columns is not None:
        df = df.with_columns(
            pl.lit(None, dtype=pl.String).alias(col) for col in columns if col not in df.columns
        ).select(columns)
    return df


def active_service_ids(z: zipfile.ZipFile, gtfs_date: date) -> pl.Series:
    """Returns the service ids with at least one active service on the given date, based on the
    `calendar.txt` and `calendar_dates.txt` tables.
    """
    import polars as pl

    day_str = gtfs_date.strftime("%Y%m%d")
    weekday = gtfs_date.strftime("%A").lower()
    active: set[str] = set()
    calendar = read_gtfs_table(z, "calendar")
    if calendar is not None:
        active |= set(
            calendar.filter(
                pl.col(weekday) == "1",
                pl.col("start_date") <= day_str,
                pl.col("end_date") >= day_str,
            )["service_id"]
        )
    calendar_dates = read_gtfs_table(z, "calendar_dates")
    if calendar_dates is not None:
        exceptions = calendar_dates.filter(pl.col("date") == day_str)
        # Exception type 1: service added, exception type 2: service removed.
        active |= set(exceptions.filter(pl.col("exception_type") == "1")["service_id"])
        active -= set(exceptions.filter(pl.col("exception_type") == "2")["service_id"])
    return pl.Series("service_id", sorted(active), dtype=pl.String)


def read_gtfs_stops_and_routes(
    gtfs_file: Path, gtfs_date: date | None
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Reads the stops and routes of a GTFS file.

    When `gtfs_date` is not `None`, only the stops and routes with at least one event on that date
    are returned.
    """
    import polars as pl

    logger.info(f"Reading GTFS file `{gtfs_file}`")
    with zipfile.ZipFile(gtfs_file) as z:
        agencies = read_gtfs_table(z, "agency", ["agency_id"])
        routes = read_gtfs_table(
            z,
            "routes",
            [
                "route_id",
                "agency_id",
                "route_short_name",
                "route_long_name",
                "route_type",
                "route_color",
            ],
        )
        trips = read_gtfs_table(z, "trips", ["route_id", "service_id", "trip_id"])
        stop_times = read_gtfs_table(z, "stop_times", ["trip_id", "stop_id"])
        stops = read_gtfs_table(
            z,
            "stops",
            ["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station"],
        )
        if any(df is None for df in (routes, trips, stop_times, stops)):
            raise MetropyError(f"Missing required table(s) in GTFS file `{gtfs_file}`")
        assert routes is not None and trips is not None
        assert stop_times is not None and stops is not None
        if gtfs_date is not None:
            trips = trips.filter(pl.col("service_id").is_in(active_service_ids(z, gtfs_date)))
            if trips.is_empty():
                logger.warning(f"No active service on {gtfs_date} in GTFS file `{gtfs_file}`")

    # The `agency_id` column is optional when the GTFS has a single agency.
    if agencies is not None and len(agencies) == 1:
        routes = routes.with_columns(pl.col("agency_id").fill_null(agencies["agency_id"][0]))
    # Routes serving each stop.
    stop_routes = (
        stop_times.join(trips.select("trip_id", "route_id"), on="trip_id", how="inner")
        .select("stop_id", "route_id")
        .unique()
        .group_by("stop_id")
        .agg(pl.col("route_id").sort().alias("route_ids"))
    )
    how = "left" if gtfs_date is None else "inner"
    stops = stops.join(stop_routes, on="stop_id", how=how).select(
        "stop_id",
        pl.col("stop_lon").cast(pl.Float64),
        pl.col("stop_lat").cast(pl.Float64),
        name="stop_name",
        location_type=pl.col("location_type")
        .str.strip_chars()
        .replace("", None)
        .fill_null("0")
        .replace_strict(LOCATION_TYPES, default=None),
        parent_station=pl.col("parent_station").replace("", None),
        route_ids=pl.col("route_ids").fill_null([]),
    )
    if gtfs_date is not None:
        routes = routes.filter(pl.col("route_id").is_in(trips["route_id"].implode()))
    routes = routes.select(
        "route_id",
        "agency_id",
        name=pl.coalesce(
            pl.col("route_short_name").replace("", None),
            pl.col("route_long_name").replace("", None),
        ),
        route_type=pl.col("route_type").cast(pl.UInt16),
        color=pl.when(pl.col("route_color").str.len_chars() > 0).then(
            pl.format("#{}", pl.col("route_color"))
        ),
    )
    return stops, routes
