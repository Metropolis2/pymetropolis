from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_pipeline import PopulationStep
from pymetropolis.metro_pipeline.steps import InputFile

from .common import DENSITY_CATS, FNC_AREA_CAT_CATS, FNC_AREA_TYPE_CATS, PURPOSES, URBAN_TYPE_CATS
from .files import (
    HouseholdsFile,
    HouseholdsHomesUrbanTypeFile,
    PersonsFile,
    ToursFile,
    TripsDestinationsFile,
    TripsDistancesFile,
    TripsFile,
    TripsOriginsFile,
    TripsUrbanTypeFile,
)

if TYPE_CHECKING:
    import geopandas as gpd
    import polars as pl


def add_lng_lat(trips: pl.DataFrame, gdf: gpd.GeoDataFrame | None, name: str):
    import polars as pl

    if gdf is not None:
        gdf.to_crs("EPSG:4326", inplace=True)
        gdf[f"{name}_lng"] = gdf.geometry.x
        gdf[f"{name}_lat"] = gdf.geometry.y
        df = pl.from_pandas(gdf[["trip_id", f"{name}_lng", f"{name}_lat"]])
        trips = trips.join(df, on="trip_id", how="left")
    else:
        trips = trips.with_columns(
            pl.lit(None, dtype=pl.Float64).alias(f"{name}_lng"),
            pl.lit(None, dtype=pl.Float64).alias(f"{name}_lat"),
        )
    return trips


class CreateToursStep(PopulationStep):
    """Generates tour-level variables from trips, persons, and households."""

    input_files = {
        "trips": TripsFile,
        "distances": TripsDistancesFile,
        "households": HouseholdsFile,
        "persons": PersonsFile,
        "home_urban_type": InputFile(HouseholdsHomesUrbanTypeFile, optional=True),
        "trip_urban_type": InputFile(TripsUrbanTypeFile, optional=True),
        "origins": InputFile(TripsOriginsFile, optional=True),
        "destinations": InputFile(TripsDestinationsFile, optional=True),
    }
    output_files = {"tours": ToursFile}

    def run(self):
        import polars as pl

        trips = self.input["trips"].read()
        distances = self.input["distances"].read()
        households = self.input["households"].read()
        persons = self.input["persons"].read()

        trips = trips.join(distances, on="trip_id", how="left")
        if self.input["trip_urban_type"].exists():
            trip_urban_type = self.input["trip_urban_type"].read()
            trips = trips.join(trip_urban_type, on="trip_id")
        else:
            trips = trips.with_columns(
                origin_density=pl.lit(None, dtype=pl.Enum(DENSITY_CATS)),
                origin_urban_type=pl.lit(None, dtype=pl.Enum(URBAN_TYPE_CATS)),
                origin_functional_area_type=pl.lit(None, dtype=pl.Enum(FNC_AREA_TYPE_CATS)),
                origin_functional_area_category=pl.lit(None, dtype=pl.Enum(FNC_AREA_CAT_CATS)),
                destination_density=pl.lit(None, dtype=pl.Enum(DENSITY_CATS)),
                destination_urban_type=pl.lit(None, dtype=pl.Enum(URBAN_TYPE_CATS)),
                destination_functional_area_type=pl.lit(None, dtype=pl.Enum(FNC_AREA_TYPE_CATS)),
                destination_functional_area_category=pl.lit(None, dtype=pl.Enum(FNC_AREA_CAT_CATS)),
            )

        trips = add_lng_lat(trips, self.input["origins"].read_if_exists(), "origin")
        trips = add_lng_lat(trips, self.input["destinations"].read_if_exists(), "destination")

        tours = (
            trips.group_by("tour_id")
            .agg(
                person_id=pl.col("person_id").first(),
                nb_trips=pl.len(),
                nb_activities=pl.len() - 1,
                origin_lngs=pl.col("origin_lng"),
                origin_lats=pl.col("origin_lat"),
                destination_lngs=pl.col("destination_lng"),
                destination_lats=pl.col("destination_lat"),
                first_purpose=pl.col("origin_purpose_group").first(),
                last_purpose=pl.col("destination_purpose_group").last(),
                purposes=pl.col("destination_purpose_group"),
                durations=pl.col("destination_activity_duration"),
                first_departure_time=pl.col("departure_time").first(),
                last_arrival_time=pl.col("arrival_time").last(),
                first_activity_start=pl.col("arrival_time").first(),
                last_activity_end=pl.col("departure_time").last(),
                travel_times=pl.col("arrival_time") - pl.col("departure_time"),
                distances=pl.col("od_distance"),
                lowest_density=pl.min_horizontal(
                    pl.col("origin_density").min(), pl.col("destination_density").min()
                ),
                highest_density=pl.max_horizontal(
                    pl.col("origin_density").max(), pl.col("destination_density").max()
                ),
                lowest_urban_type=pl.min_horizontal(
                    pl.col("origin_urban_type").min(), pl.col("destination_urban_type").min()
                ),
                highest_urban_type=pl.max_horizontal(
                    pl.col("origin_urban_type").max(), pl.col("destination_urban_type").max()
                ),
                lowest_functional_area_type=pl.min_horizontal(
                    pl.col("origin_functional_area_type").min(),
                    pl.col("destination_functional_area_type").min(),
                ),
                highest_functional_area_type=pl.max_horizontal(
                    pl.col("origin_functional_area_type").max(),
                    pl.col("destination_functional_area_type").max(),
                ),
                lowest_functional_area_category=pl.min_horizontal(
                    pl.col("origin_functional_area_category").min(),
                    pl.col("destination_functional_area_category").min(),
                ),
                highest_functional_area_category=pl.max_horizontal(
                    pl.col("origin_functional_area_category").max(),
                    pl.col("destination_functional_area_category").max(),
                ),
            )
            .with_columns(
                purposes=pl.col("purposes").list.slice(0, pl.len() - 1),
                durations=pl.col("durations").list.slice(0, pl.len() - 1),
            )
            .with_columns(
                total_tour_duration=pl.col("last_arrival_time") - pl.col("first_departure_time"),
                total_activity_duration=pl.col("durations").list.sum(),
                total_travel_time=pl.col("travel_times").list.sum(),
                total_distance=pl.col("distances").list.sum(),
            )
            .with_columns(
                pl.col("purposes").list.contains(purpose).alias(f"has_{purpose}_purpose")
                for purpose in PURPOSES
            )
            .with_columns(nb_tours=pl.len().over("person_id"))
        )

        households = households.select(
            "household_id",
            # "household_type",  # TODO
            "nb_cars",
            "nb_motorcycles",
            "nb_bicycles",
            "nb_persons",
            "nb_majors",
            "nb_minors",
            "nb_driving_licenses",
            minor_ratio=pl.col("nb_minors") / pl.col("nb_persons"),
            car_ratio=pl.col("nb_cars") / pl.col("nb_persons"),
            driving_license_ratio=pl.col("nb_cars")
            / pl.col("nb_driving_licenses").clip(lower_bound=1),
        )
        if self.input["home_urban_type"].exists():
            home_urban_type = self.input["home_urban_type"].read()
            households = households.join(home_urban_type, on="household_id")

        persons = (
            persons.select(
                "household_id",
                "person_id",
                "woman",
                "age",
                "education_level",
                "professional_activity",
                "socioprofessional_class",
                "has_driving_license",
                "has_public_transit_subscription",
                nb_women=pl.col("woman").sum().over("household_id"),
                nb_major_women=pl.col("woman")
                .and_(pl.col("age").ge(18))
                .sum()
                .over("household_id"),
                nb_men=pl.col("woman").not_().sum().over("household_id"),
                nb_major_men=pl.col("woman")
                .not_()
                .and_(pl.col("age").ge(18))
                .sum()
                .over("household_id"),
                oldest_age_diff=pl.col("age").max().over("household_id") - pl.col("age"),
            )
            .join(households, on="household_id")
            .drop("household_id")
        )

        tours = tours.join(persons, on="person_id", how="left").drop("person_id")

        self.output["tours"].write(tours)
