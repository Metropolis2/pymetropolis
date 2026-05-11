from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.io import read_dataframe
from pymetropolis.metro_demand.routing.files import TripsRoadNodesFile
from pymetropolis.metro_pipeline.parameters import PathParameter
from pymetropolis.metro_pipeline.steps import Step
from pymetropolis.metro_spatial import GeoStep

from .files import PersonsFile, TripsDestinationsFile, TripsFile, TripsOriginsFile


class GenericPopulationStep(Step):
    """Generates a population (persons and trips) from a list of car-driver origin-destination
    pairs.

    Each person has a single trip.
    """

    input_files = {"road_ods": TripsRoadNodesFile}
    output_files = {"trips": TripsFile, "persons": PersonsFile}
    priority = 0

    def run(self):
        import polars as pl

        df: pl.DataFrame = self.input["road_ods"].read()
        trips = df.select(
            "trip_id",
            person_id="trip_id",
            household_id="trip_id",
            trip_index=pl.lit(1, dtype=pl.UInt8),
            tour_id="trip_id",
        )
        persons = trips.select(
            "person_id",
            "household_id",
            person_index=pl.lit(1, dtype=pl.UInt8),
            has_driving_license=pl.lit(True, dtype=pl.Boolean),
            has_public_transit_subscription=pl.lit(True, dtype=pl.Boolean),
        )
        self.output["trips"].write(trips)
        self.output["persons"].write(persons)


class PopulationFromTripCoordinatesStep(GeoStep):
    """Generates a population (persons and trips) from a list of trips with origin / destination
    coordinates.

    Each person has a single trip.

    The input file must have columns: `trip_id`, `origin_lng`, `origin_lat`, `destination_lng`,
    `destination_lat`.
    """

    trip_coordinates_file = PathParameter(
        "population.trip_coordinates_file",
        check_file_exists=True,
        description="Path to a Parquet / CSV file with coordinates of each trip.",
    )
    output_files = {
        "trips": TripsFile,
        "persons": PersonsFile,
        "origins": TripsOriginsFile,
        "destinations": TripsDestinationsFile,
    }

    def is_defined(self):
        return self.trip_coordinates_file is not None

    def run(self):
        import geopandas as gpd
        import polars as pl

        df = read_dataframe(self.trip_coordinates_file)
        for col in ("trip_id", "origin_lng", "origin_lat", "destination_lng", "destination_lat"):
            if col not in df.columns:
                raise MetropyError(
                    f"Missing column `{col}` in input file `{self.trip_coordinates_file}`."
                )
        if df["trip_id"].n_unique() != len(df):
            raise MetropyError("Trip ids must be unique.")
        origins = gpd.GeoSeries.from_xy(df["origin_lng"], df["origin_lat"], crs="EPSG:4326")
        origins_gdf = gpd.GeoDataFrame({"trip_id": df["trip_id"]}, geometry=origins).to_crs(
            self.crs
        )
        destinations = gpd.GeoSeries.from_xy(
            df["destination_lng"], df["destination_lat"], crs="EPSG:4326"
        )
        destinations_gdf = gpd.GeoDataFrame(
            {"trip_id": df["trip_id"]}, geometry=destinations
        ).to_crs(self.crs)
        trips = df.select(
            "trip_id",
            person_id="trip_id",
            household_id="trip_id",
            trip_index=pl.lit(1, dtype=pl.UInt8),
            tour_id="trip_id",
        )
        persons = trips.select(
            "person_id",
            "household_id",
            person_index=pl.lit(1, dtype=pl.UInt8),
            has_driving_license=pl.lit(True, dtype=pl.Boolean),
            has_public_transit_subscription=pl.lit(True, dtype=pl.Boolean),
        )
        self.output["origins"].write(origins_gdf)
        self.output["destinations"].write(destinations_gdf)
        self.output["trips"].write(trips)
        self.output["persons"].write(persons)
