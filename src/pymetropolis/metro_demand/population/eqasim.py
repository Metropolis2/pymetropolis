from pathlib import Path

import duckdb
import geopandas as gpd
import polars as pl
from loguru import logger
from shapely import wkb
from shapely.geometry import Polygon

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.utils import find_file, seconds_since_midnight_to_datetime_pl
from pymetropolis.metro_pipeline.parameters import FractionParameter, PathParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_spatial import GeoStep
from pymetropolis.metro_spatial.simulation_area import SimulationAreaFile
from pymetropolis.random import RandomStep

from .files import (
    HouseholdsFile,
    HouseholdsHomesFile,
    PersonsFile,
    TripsDestinationsFile,
    TripsFile,
    TripsOriginsFile,
)


def read_households(
    parquet_file: Path | None = None, csv_file: Path | None = None, household_ids: set | None = None
) -> pl.DataFrame:
    if parquet_file:
        logger.info(f"Reading households from `{parquet_file}`")
        df = pl.scan_parquet(parquet_file)
    else:
        assert csv_file is not None
        logger.info(f"Reading households from `{csv_file}`")
        df = pl.scan_csv(csv_file, separator=";")
    if household_ids:
        df = df.filter(pl.col("household_id").is_in(household_ids))
    df = df.sort("household_id")
    df = df.select(
        pl.col("household_id").cast(pl.UInt64),
        pl.col("income").cast(pl.Float64),
        # nb_cars="number_of_cars",
        # nb_motorcycles="number_of_motorcycles",
        nb_bicycles="number_of_bikes",
    )
    # "household_type",
    return df.collect()  # ty: ignore[invalid-return-type]


def read_persons(
    parquet_file: Path | None = None, csv_file: Path | None = None, household_ids: set | None = None
) -> pl.DataFrame:
    if parquet_file:
        logger.info(f"Reading persons from `{parquet_file}`")
        df = pl.scan_parquet(parquet_file)
    else:
        assert csv_file is not None
        logger.info(f"Reading persons from `{csv_file}`")
        df = pl.scan_csv(csv_file, separator=";")
    if household_ids:
        df = df.filter(pl.col("household_id").is_in(household_ids))
    df = df.sort("person_id")
    df = df.select(
        pl.col("person_id").cast(pl.UInt64),
        pl.col("household_id").cast(pl.UInt64),
        person_index=pl.int_range(1, pl.len() + 1, dtype=pl.UInt8).over("household_id"),
        woman=pl.col("sex") == "female",
        age=pl.col("age").cast(pl.UInt8),
        detailed_education_level=pl.col("detailed_education_level").cast(pl.String),
        education_level=pl.col("education_level").cast(pl.String),
        # professional_activity="professional_activity",
        socioprofessional_class=pl.col("socioprofessional_class").cast(pl.UInt8),
        has_driving_license="has_driving_license",
        has_public_transit_subscription="has_pt_subscription",
    )
    # "reference_person_link",
    return df.collect()  # ty: ignore[invalid-return-type]


def read_trips(
    geoparquet_file: Path | None = None,
    gpkg_file: Path | None = None,
    person_ids: set | None = None,
) -> tuple[pl.DataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")
    if geoparquet_file:
        logger.info(f"Reading trips from `{geoparquet_file}`")
        lf = pl.scan_parquet(geoparquet_file)
        source = f"read_parquet('{geoparquet_file}')"
        geom = "geometry"
    else:
        assert gpkg_file is not None
        logger.info(f"Reading trips from `{gpkg_file}`")
        query = f"""
            SELECT
                person_id,
                trip_index,
                departure_time,
                arrival_time,
                preceding_purpose,
                following_purpose
            FROM ST_Read('{gpkg_file}')
        """
        lf = con.execute(query).pl().lazy()
        source = f"ST_Read('{gpkg_file}')"
        geom = "geom"
    # Clean trips.
    if person_ids:
        lf = lf.filter(pl.col("person_id").is_in(person_ids))
    lf = lf.sort("person_id", "trip_index")
    lf = lf.select(
        trip_id=pl.format("{}-{}", "person_id", "trip_index"),
        person_id="person_id",
        trip_index=pl.col("trip_index").cast(pl.UInt8) + 1,
        origin_purpose_group="preceding_purpose",
        destination_purpose_group="following_purpose",
        departure_time=seconds_since_midnight_to_datetime_pl("departure_time"),
        arrival_time=seconds_since_midnight_to_datetime_pl("departure_time"),
    ).with_columns(
        # `tour_id` is `{person_id}-{home_sequence_idx}`, where `home_sequence_idx` is the number of
        # times the "home" purpose occured so far for this person.
        tour_id=pl.format(
            "{}-{}",
            pl.col("person_id"),
            pl.col("origin_purpose_group").eq("home").cum_sum().over("person_id"),
        ),
        origin_activity_duration=pl.col("departure_time")
        - pl.col("arrival_time").shift(1).over("person_id"),
        destination_activity_duration=pl.col("departure_time").shift(-1).over("person_id")
        - pl.col("arrival_time"),
    )
    trips: pl.DataFrame = lf.collect()  # ty: ignore[invalid-assignment]
    # Load origins / destinations.
    query = f"""
        SELECT
            format('{{}}-{{}}', person_id, trip_index) AS trip_id,
            ST_AsWKB(ST_StartPoint({geom}))            AS origin_wkb,
            ST_AsWKB(ST_EndPoint({geom}))              AS destination_wkb
        FROM {source}
    """
    df = con.execute(query).pl()
    # Note. Eqasim uses EPSG 2154 CRS.
    origins = gpd.GeoDataFrame(
        {"trip_id": df["trip_id"]},
        geometry=gpd.GeoSeries.from_wkb(df["origin_wkb"], crs="EPSG:2154"),
    )
    origins = origins.loc[origins["trip_id"].isin(trips["trip_id"]), :]
    destinations = gpd.GeoDataFrame(
        {"trip_id": df["trip_id"]},
        geometry=gpd.GeoSeries.from_wkb(df["destination_wkb"], crs="EPSG:2154"),
    )
    destinations = destinations.loc[destinations["trip_id"].isin(trips["trip_id"]), :]
    return trips, origins, destinations


def read_homes(
    geoparquet_file: Path | None = None,
    gpkg_file: Path | None = None,
    filter_polygon: Polygon | None = None,
    fraction: float = 1.0,
    random_seed: int | None = None,
) -> gpd.GeoDataFrame:
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")
    if geoparquet_file:
        logger.info(f"Reading homes from `{geoparquet_file}`")
        source = f"read_parquet('{geoparquet_file}')"
        geom = "geometry"
    else:
        assert gpkg_file is not None
        logger.info(f"Reading homes from `{gpkg_file}`")
        source = f"ST_Read('{gpkg_file}')"
        geom = "geom"
    query = f"""
        SELECT
            household_id,
            ST_AsWKB({geom}) AS geometry
        FROM {source}
    """
    if filter_polygon is not None:
        wkb_hex = wkb.dumps(filter_polygon, hex=True)
        query += f"WHERE ST_Within({geom}, ST_GeomFromHEXWKB('{wkb_hex}'))"
    df = con.execute(query).pl()
    if df.is_empty():
        raise MetropyError("No household within the simulation area")
    if fraction != 1.0:
        assert fraction < 1.0 and fraction >= 0.0
        df = df.sample(fraction=fraction, seed=random_seed)
    if df.is_empty():
        raise MetropyError("No household selected, choose a larger fraction")
    # Note. Eqasim uses EPSG 2154 CRS.
    homes = gpd.GeoDataFrame(
        {"household_id": df["household_id"]},
        geometry=gpd.GeoSeries.from_wkb(df["geometry"], crs="EPSG:2154"),
    )
    return homes


def clean(
    households: pl.DataFrame, persons: pl.DataFrame, trips: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    households = households.join(
        persons.group_by("household_id").agg(
            nb_persons=pl.len(),
            nb_persons_5plus=pl.col("age").ge(5).sum(),
            nb_majors=pl.col("age").ge(18).sum(),
            nb_minors=pl.col("age").lt(18).sum(),
            nb_driving_licenses=pl.col("has_driving_license").sum(),
        ),
        on="household_id",
        how="left",
    )
    persons = persons.join(
        trips.group_by("person_id").agg(nb_trips=pl.len()), on="person_id", how="left"
    ).with_columns(pl.col("nb_trips").fill_null(0))
    trips = trips.join(persons.select("person_id", "household_id"), on="person_id", how="left")
    return households, persons, trips


class EqasimImportStep(GeoStep, RandomStep):
    """Imports a synthetic population from the output of the Eqasim pipeline.

    To use this step, you first need to generate a synthetic population following the instructions
    here: [https://github.com/eqasim-org/eqasim-france](https://github.com/eqasim-org/eqasim-france).

    A few extra steps need to be done for Pymetropolis:

    - Set the `departements` or `regions` configuration parameter so that it englobs the simulation
      area. Pymetropolis will automatically restricts the population to the simulation area (if
      defined).
    - Use parquet and geoparquet as output formats:
      ```yaml
      output_formats:
        - "parquet"
        - "geoparquet"
      ```
    - Use MobiSurvStd for the Household Travel Survey:
      ```yaml
      hts: mobisurvstd
      mobisurvstd:
        path: path/to/compatible/hts/
      ```
    - Use `urban_type` and `professional_activity` as extra matching attributes:
      ```yaml
      use_urban_type: true
      matching_attributes: ["professional_activity", "urban_type", "*default*"]
      ```
    - Add `"escort"` as an activity purpose:
      ```yaml
      escort_purpose: true
      ```
    - Do not activate mode choice (unused):
      ```yaml
      mode_choice: false
      ```
    """

    eqasim_output = PathParameter(
        "synthetic_population.eqasim_output",
        check_dir_exists=True,
        description="Path to the output directory of the Eqasim synthetic population pipeline.",
    )
    fraction = FractionParameter(
        "synthetic_population.fraction",
        default=1.0,
        description="Fraction of the synthetic population to be selected for simulations.",
        note=(
            "If the synthetic population already represents a part of the total population (with "
            "Eqasim's `sampling_rate` parameter), you probably want to keep this parameter to 1."
        ),
    )

    input_files = {"simulation_area": InputFile(SimulationAreaFile, optional=True)}
    output_files = {
        "households": HouseholdsFile,
        "homes": HouseholdsHomesFile,
        "persons": PersonsFile,
        "trips": TripsFile,
        "origins": TripsOriginsFile,
        "destinations": TripsDestinationsFile,
    }

    def is_defined(self) -> bool:
        return self.eqasim_output is not None

    def run(self):
        path = self.eqasim_output
        households_parquet = find_file("*_households.parquet", path)
        households_csv = find_file("*_households.csv", path)
        persons_parquet = find_file("*_persons.parquet", path)
        persons_csv = find_file("*_persons.csv", path)
        trips_geoparquet = find_file("*_trips.geoparquet", path)
        trips_gpkg = find_file("*_trips.gpkg", path)
        homes_geoparquet = find_file("*_homes.geoparquet", path)
        homes_gpkg = find_file("*_homes.gpkg", path)
        if not households_parquet and not households_csv:
            raise MetropyError(f"No households file in `{path}` directory")
        if not persons_parquet and not persons_csv:
            raise MetropyError(f"No persons file in `{path}` directory")
        if not trips_geoparquet and not trips_gpkg:
            raise MetropyError(f"No trips geofile in `{path}` directory")
        if not homes_geoparquet and not homes_gpkg:
            raise MetropyError(f"No homes geofile in `{path}` directory")
        homes = read_homes(
            geoparquet_file=homes_geoparquet,
            gpkg_file=homes_gpkg,
            filter_polygon=self.input["simulation_area"].get_area_opt(),  # ty: ignore[unresolved-attribute]
            fraction=self.fraction,
            random_seed=self.random_seed,
        )
        household_ids = set(homes["household_id"])
        households = read_households(
            parquet_file=households_parquet, csv_file=households_csv, household_ids=household_ids
        )
        persons = read_persons(
            parquet_file=persons_parquet, csv_file=persons_csv, household_ids=household_ids
        )
        person_ids = set(persons["person_id"])
        trips, origins, destinations = read_trips(
            geoparquet_file=trips_geoparquet, gpkg_file=trips_gpkg, person_ids=person_ids
        )
        households, persons, trips = clean(households, persons, trips)
        homes = homes.to_crs(self.crs)
        origins = origins.to_crs(self.crs)
        destinations = destinations.to_crs(self.crs)
        logger.debug(
            f"Imported {len(households):,} households, {len(persons):,} persons, "
            f"{len(trips):,} trips"
        )
        self.output["households"].write(households)
        self.output["homes"].write(homes)
        self.output["persons"].write(persons)
        self.output["trips"].write(trips)
        self.output["origins"].write(origins)
        self.output["destinations"].write(destinations)
