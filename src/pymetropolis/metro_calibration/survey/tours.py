from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_demand.population.common import (
    DENSITY_CATS,
    FNC_AREA_CAT_CATS,
    FNC_AREA_TYPE_CATS,
    PURPOSES,
    URBAN_TYPE_CATS,
)
from pymetropolis.metro_pipeline import Step

from .files import (
    SurveyedHouseholdsFile,
    SurveyedLegsFile,
    SurveyedPersonsFile,
    SurveyedToursFile,
    SurveyedTripsFile,
)

if TYPE_CHECKING:
    import polars as pl


def read_tours(
    households: pl.DataFrame, persons: pl.DataFrame, trips: pl.DataFrame, legs: pl.DataFrame
):
    import polars as pl

    # Clean household-level variables.
    households = households.select(
        "household_id",
        "nb_cars",
        "nb_motorcycles",
        "nb_bicycles",
        "nb_persons",
        "nb_majors",
        "nb_minors",
        home_density=pl.col("home_insee_density").cast(pl.String).cast(pl.Enum(DENSITY_CATS)),
        home_urban_type=pl.col("home_insee_urban_type").cast(pl.Enum(URBAN_TYPE_CATS)),
        home_functional_area_type=pl.col("home_insee_aav_type")
        .cast(pl.String)
        .cast(pl.Enum(FNC_AREA_TYPE_CATS)),
        home_functional_area_category=pl.col("home_aav_category")
        .cast(pl.String)
        .cast(pl.Enum(FNC_AREA_CAT_CATS)),
        household_type=pl.when(household_type="couple:no_child")
        .then(pl.lit("couple"))
        .when(household_type="couple:children")
        .then(pl.lit("couple_family"))
        .when(pl.col("household_type").is_in(("singleparent:mother", "singleparent:father")))
        .then(pl.lit("singleparent"))
        .when(pl.col("household_type").is_in(("single:woman", "single:man")))
        .then(pl.lit("single"))
        .otherwise(pl.lit("other")),
    )

    # Clean person-level variables.
    persons = persons.select(
        "person_id",
        "woman",
        "age",
        pl.col("education_level").cast(pl.String),
        "has_public_transit_subscription",
        oldest_age_diff=pl.col("age").max().over("household_id") - pl.col("age"),
        professional_activity=pl.when(pl.col("age") <= 14)
        .then(pl.lit("under14"))
        .when(professional_occupation="student")
        .then(pl.lit("student"))
        .when(detailed_professional_occupation="worker:part_time")
        .then(pl.lit("part_time_worker"))
        .when(professional_occupation="worker")
        .then(pl.lit("full_time_worker"))
        .when(detailed_professional_occupation="other:retired")
        .then(pl.lit("retired"))
        .otherwise(pl.lit("other")),
        socioprofessional_class=pl.when(
            pl.col("pcs_group_code") <= 6, professional_occupation="worker"
        ).then(pl.col("pcs_group_code")),
        has_driving_license=pl.col("has_driving_license").eq("yes"),
        nb_surveyed=pl.col("is_surveyed").sum().over("household_id"),
        nb_women=pl.col("woman").sum().over("household_id"),
        nb_major_women=(pl.col("woman") & pl.col("age").ge(18)).sum().over("household_id"),
        nb_men=pl.col("woman").not_().sum().over("household_id"),
        nb_major_men=(pl.col("woman").not_() & pl.col("age").ge(18)).sum().over("household_id"),
        nb_driving_licenses=pl.col("has_driving_license").eq("yes").sum().over("household_id"),
        weight="sample_weight_surveyed",
    )

    # Clean trip-level variables.
    trips = trips.select(
        "trip_id",
        "person_id",
        "household_id",
        "home_sequence_index",
        pl.col("origin_purpose_group").cast(pl.String),
        pl.col("destination_purpose_group").cast(pl.String),
        pl.col("origin_insee_density").cast(pl.String).cast(pl.Enum(DENSITY_CATS)),
        pl.col("origin_insee_urban_type").cast(pl.Enum(URBAN_TYPE_CATS)),
        pl.col("origin_insee_aav_type").cast(pl.String).cast(pl.Enum(FNC_AREA_TYPE_CATS)),
        pl.col("origin_aav_category").cast(pl.String).cast(pl.Enum(FNC_AREA_CAT_CATS)),
        pl.col("destination_insee_density").cast(pl.String).cast(pl.Enum(DENSITY_CATS)),
        pl.col("destination_insee_urban_type").cast(pl.Enum(URBAN_TYPE_CATS)),
        pl.col("destination_insee_aav_type").cast(pl.String).cast(pl.Enum(FNC_AREA_TYPE_CATS)),
        pl.col("destination_aav_category").cast(pl.String).cast(pl.Enum(FNC_AREA_CAT_CATS)),
        pl.col("trip_weekday").cast(pl.String),
        "trip_euclidean_distance_km",
        "trip_perimeter",
        destination_activity_duration=pl.duration(minutes="destination_activity_duration").alias(
            "destination_activity_duration"
        ),
        # Round departure / arrival time to nearest 5 minutes.
        departure_time=pl.duration(minutes=((pl.col("departure_time") / 5).round() * 5)),
        arrival_time=pl.duration(minutes=((pl.col("arrival_time") / 5).round() * 5)),
    ).with_columns(travel_time=pl.col("arrival_time") - pl.col("departure_time"))

    # Clean leg-level variables.
    legs = legs.select(
        "trip_id",
        "leg_euclidean_distance_km",
        # When there is no info, it is assumed that people are travelling alone in the car.
        mode=pl.when(pl.col("nb_persons_in_vehicle").fill_null(1).eq(1), mode_group="car_driver")
        .then(pl.lit("car_driver_alone"))
        .when(mode_group="car_driver")
        .then(pl.lit("car_driver_with_passengers"))
        .otherwise(pl.col("mode_group").cast(pl.String)),
    )
    modes = legs["mode"].unique()
    car_modes = [m for m in modes if "car_" in m]
    trip_legs = legs.group_by("trip_id").agg(
        pl.len().alias("nb_legs"),
        *(pl.col("mode").eq(m).sum().alias(f"{m}_count") for m in modes),
        *(
            # Compute euclidean distance by mode, filter for non NULL values so that NULL do not
            # propagate.
            pl.col("leg_euclidean_distance_km")
            .filter(pl.col("leg_euclidean_distance_km").is_not_null(), mode=m)
            .sum()
            .alias(f"{m}_distance")
            for m in modes
        ),
    )
    # Identify trip-level mode.
    trip_legs = trip_legs.with_columns(trip_mode=pl.lit(None, dtype=pl.String))
    for mode in modes:
        # All legs have that mode.
        trip_legs = trip_legs.with_columns(
            trip_mode=pl.when(pl.col("nb_legs") == pl.col(f"{mode}_count"))
            .then(pl.lit(mode))
            .otherwise("trip_mode")
        )
        if mode == "walking":
            continue
        # All legs without that mode are walking leg, with NULL or <500m total walking distance.
        trip_legs = trip_legs.with_columns(
            trip_mode=pl.when(
                pl.col("nb_legs") == pl.col(f"{mode}_count") + pl.col("walking_count"),
                pl.col(f"{mode}_count"),
                pl.col("walking_distance").fill_null(0.0) < 0.5,
            )
            .then(pl.lit(mode))
            .otherwise("trip_mode")
        )
    for mode in car_modes:
        # Identify park-and-ride trips.
        # Note. For now, all car modes (driver, passenger, etc.) are eligible for park-and-ride.
        trip_legs = trip_legs.with_columns(
            trip_mode=pl.when(
                pl.col("nb_legs")
                == pl.col(f"{mode}_count")
                + pl.col("public_transit_count")
                + pl.col("walking_count"),
                pl.col(f"{mode}_count") > 0,
                pl.col("public_transit_count") > 0,
            )
            .then(pl.lit("park_and_ride"))
            .otherwise("trip_mode")
        )
    # Identify public transit trips (with walking legs).
    trip_legs = trip_legs.with_columns(
        trip_mode=pl.when(
            pl.col("nb_legs") == pl.col("public_transit_count") + pl.col("walking_count"),
            pl.col("public_transit_count") > 0,
        )
        .then(pl.lit("public_transit"))
        .otherwise("trip_mode")
    )
    # Identify bike-and-ride trips.
    trip_legs = trip_legs.with_columns(
        trip_mode=pl.when(
            pl.col("nb_legs")
            == pl.col("bicycle_count") + pl.col("public_transit_count") + pl.col("walking_count"),
            pl.col("bicycle_count") > 0,
            pl.col("public_transit_count") > 0,
        )
        .then(pl.lit("bike_and_ride"))
        .otherwise("trip_mode")
    )
    # Identify car driver / passenger combination.
    trip_legs = trip_legs.with_columns(
        trip_mode=pl.when(
            pl.col("nb_legs")
            == pl.col("car_driver_alone_count")
            + pl.col("car_driver_with_passengers_count")
            + pl.col("car_passenger_count")
            + pl.col("walking_count"),
            (pl.col("car_driver_alone_count") > 0)
            | (pl.col("car_driver_with_passengers_count") > 0),
            pl.col("car_passenger_count") > 0,
            pl.col("walking_distance").fill_null(0.0) < 0.5,
        )
        .then(pl.lit("car_mixed"))
        .otherwise("trip_mode")
    )
    # Note. `car_mixed` trips are set to the most observed mode (driver or passenger), for lack of
    # a better methodology.
    # Flag `car_mixed` trips.
    trip_legs = trip_legs.with_columns(is_car_mixed=pl.col("trip_mode") == "car_mixed")
    for mode in car_modes:
        trip_legs = trip_legs.with_columns(
            trip_mode=pl.when(
                # Mode distance is not smaller than any distance for other car modes.
                *(pl.col(f"{mode}_distance") >= pl.col(f"{m}_distance") for m in car_modes),
                trip_mode="car_mixed",
            )
            .then(pl.lit(mode))
            .otherwise("trip_mode")
        )
    # At this point `trip_mode` = NULL for non-standard combinations (3+ modes, bicycle+car, etc.).
    trip_legs = trip_legs.select("trip_id", "trip_mode", "is_car_mixed")

    # Join trips and legs..
    trips = trips.join(trip_legs, on="trip_id").sort("trip_id")

    tours = (
        trips.group_by("household_id", "person_id", "home_sequence_index")
        .agg(
            "origin_insee_density",
            "origin_insee_urban_type",
            "origin_insee_aav_type",
            "origin_aav_category",
            "destination_insee_density",
            "destination_insee_urban_type",
            "destination_insee_aav_type",
            "destination_aav_category",
            nb_trips=pl.len(),
            nb_activities=pl.len() - 1,
            modes="trip_mode",
            first_purpose=pl.col("origin_purpose_group").first(),
            last_purpose=pl.col("destination_purpose_group").last(),
            purposes=pl.col("destination_purpose_group"),
            durations=pl.col("destination_activity_duration"),
            first_departure_time=pl.col("departure_time").first(),
            last_arrival_time=pl.col("arrival_time").last(),
            first_activity_start=pl.col("arrival_time").first(),
            last_activity_end=pl.col("departure_time").last(),
            travel_times=pl.col("travel_time"),
            distances=pl.col("trip_euclidean_distance_km") * 1000,  # Convert to meters.
            trip_weekday=pl.col("trip_weekday").first(),  # They should be unique.
            outside_perimeter=pl.col("trip_perimeter").ne("internal").any(),
        )
        .with_columns(
            # Drop last purpose / activity duration (should be home).
            pl.col("purposes").list.slice(0, pl.len() - 1),
            pl.col("durations").list.slice(0, pl.len() - 1),
        )
        .with_columns(
            total_tour_duration=pl.col("last_arrival_time") - pl.col("first_departure_time"),
            total_activity_duration=pl.col("durations").list.sum(),
            total_travel_time=pl.col("travel_times").list.sum(),
            total_distance=pl.col("distances").list.sum(),
        )
        .with_columns(
            *(
                pl.col("purposes").list.contains(purpose).alias(f"has_{purpose}_purpose")
                for purpose in PURPOSES
            )
        )
        .with_columns(
            # Note. The min / max works for variables below since they are of type enum
            # (with the enum modalities being properly ordered).
            lowest_density=pl.min_horizontal(
                pl.col("origin_insee_density").list.min(),
                pl.col("destination_insee_density").list.min(),
            ),
            highest_density=pl.max_horizontal(
                pl.col("origin_insee_density").list.max(),
                pl.col("destination_insee_density").list.max(),
            ),
            lowest_urban_type=pl.min_horizontal(
                pl.col("origin_insee_urban_type").list.min(),
                pl.col("destination_insee_urban_type").list.min(),
            ),
            highest_urban_type=pl.max_horizontal(
                pl.col("origin_insee_urban_type").list.max(),
                pl.col("destination_insee_urban_type").list.max(),
            ),
            lowest_functional_area_type=pl.min_horizontal(
                pl.col("origin_insee_aav_type").list.min(),
                pl.col("destination_insee_aav_type").list.min(),
            ),
            highest_functional_area_type=pl.max_horizontal(
                pl.col("origin_insee_aav_type").list.max(),
                pl.col("destination_insee_aav_type").list.max(),
            ),
            lowest_functional_area_category=pl.min_horizontal(
                pl.col("origin_aav_category").list.min(),
                pl.col("destination_aav_category").list.min(),
            ),
            highest_functional_area_category=pl.max_horizontal(
                pl.col("origin_aav_category").list.max(),
                pl.col("destination_aav_category").list.max(),
            ),
        )
        .drop(
            "origin_insee_density",
            "origin_insee_urban_type",
            "origin_insee_aav_type",
            "origin_aav_category",
            "destination_insee_density",
            "destination_insee_urban_type",
            "destination_insee_aav_type",
            "destination_aav_category",
        )
    )

    tours = (
        tours.join(households, on="household_id")
        .join(persons, on="person_id")
        .with_columns(
            minor_ratio=pl.col("nb_minors") / pl.col("nb_persons"),
            car_ratio=pl.col("nb_cars") / pl.col("nb_persons"),
            driving_license_ratio=pl.col("nb_cars")
            / pl.col("nb_driving_licenses").clip(lower_bound=1),
            household_fully_surveyed=pl.col("nb_surveyed") == pl.col("nb_persons"),
            household_surveyed_on_same_day=pl.col("trip_weekday")
            .n_unique()
            .over("household_id")
            .eq(1),
            nb_tours=pl.len().over("person_id"),
            weighted_distance=pl.col("total_distance") * pl.col("weight"),
        )
        .sort("person_id", "home_sequence_index")
    )

    # Find main mode at the tour level.
    # To reduce the occurence of "mixed", we discard the walking mode if total walking distance is
    # smaller than 1km.
    tours = (
        tours.with_columns(
            tmp_modes=pl.when(
                pl.col("modes").list.len() >= 2,
                pl.col("distances")
                .list.gather(
                    pl.col("modes").list.eval(
                        pl.int_range(pl.len()).filter(pl.element() == "walking")
                    )
                )
                .list.sum()
                < 1000,
            )
            .then(pl.col("modes").list.filter(pl.element() != "walking"))
            .otherwise("modes")
        )
        .with_columns(
            tour_mode=pl.when(pl.col("tmp_modes").list.n_unique() == 1)
            .then(pl.col("tmp_modes").list.first())
            .when(
                pl.col("tmp_modes")
                .list.eval(pl.element().str.starts_with("car_driver_"))
                .list.all()
            )
            .then(pl.lit("car_driver_mixed"))
            .when(
                pl.col("tmp_modes").list.contains("public_transit")
                & pl.col("tmp_modes").list.contains("park_and_ride")
            )
            .then(pl.lit("park_and_ride"))
            .when(pl.col("tmp_modes").list.contains(None).not_())
            .then(pl.lit("mixed"))
        )
        .drop("tmp_modes")
    )
    # At this point, `tour_mode` = NULL when some trip-level modes are unknown.

    matching_cols = [
        "household_id",
        "first_departure_time",
        "last_arrival_time",
        "purposes",
        "distances",
    ]
    joint_tours = (
        tours.join(
            tours.select(
                *matching_cols,
                *[
                    pl.col("person_id").alias("other_person_id"),
                    pl.col("tour_mode").alias("other_mode"),
                ],
            ),
            on=matching_cols,
            how="inner",
        )
        .filter(pl.col("person_id") != pl.col("other_person_id"))
        .group_by("person_id", "home_sequence_index")
        .agg(other_person_ids="other_person_id", other_modes="other_mode")
    )
    tours = tours.join(
        joint_tours, on=["person_id", "home_sequence_index"], how="left"
    ).with_columns(joint_tour=pl.col("other_person_ids").is_not_null())

    # Create tour_id column.
    tours = tours.with_columns(
        tour_id=pl.concat_str("person_id", pl.lit("-"), "home_sequence_index")
    ).drop("household_id", "person_id", "home_sequence_index")

    return tours


class CleanSurveyToursStep(Step):
    """Creates variables at the home-based tour level from survey data."""

    input_files = {
        "households": SurveyedHouseholdsFile,
        "persons": SurveyedPersonsFile,
        "trips": SurveyedTripsFile,
        "legs": SurveyedLegsFile,
    }
    output_files = {"tours": SurveyedToursFile}

    def run(self):
        households = self.input["households"].read()
        persons = self.input["persons"].read()
        trips = self.input["trips"].read()
        legs = self.input["legs"].read()

        tours = read_tours(households, persons, trips, legs)

        self.output["tours"].write(tours)
