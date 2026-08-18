from pymetropolis.metro_pipeline import Step

from .common import PURPOSES
from .files import HouseholdsFile, PersonsFile, ToursFile, TripsDistancesFile, TripsFile


class CreateToursStep(Step):
    """Generates tour-level variables from trips, persons, and households."""

    input_files = {
        "trips": TripsFile,
        "distances": TripsDistancesFile,
        "households": HouseholdsFile,
        "persons": PersonsFile,
    }
    output_files = {"tours": ToursFile}

    def run(self):
        import polars as pl

        trips = self.input["trips"].read()
        distances = self.input["distances"].read()
        households = self.input["households"].read()
        persons = self.input["persons"].read()

        trips = trips.join(distances, on="trip_id", how="left")

        tours = (
            trips.group_by("tour_id")
            .agg(
                person_id=pl.col("person_id").first(),
                nb_trips=pl.len(),
                nb_activities=pl.len() - 1,
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
            .join(
                households.select(
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
                ),
                on="household_id",
            )
            .drop("household_id")
        )

        tours = tours.join(persons, on="person_id", how="left").drop("person_id")

        self.output["tours"].write(tours)

        # "lowest_density",
        # "highest_density",
        # "lowest_urban_type",
        # "highest_urban_type",
        # "lowest_functional_area_type",
        # "highest_functional_area_type",
        # "lowest_functional_area_category",
        # "highest_functional_area_category",
        # "home_density",
        # "home_urban_type",
        # "home_functional_area_type",
        # "home_functional_area_category",
