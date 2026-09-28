from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.errors import error_context
from pymetropolis.metro_common.utils import pl_duration_to_seconds
from pymetropolis.metro_demand.departure_time import LinearScheduleFile, TstarsFile
from pymetropolis.metro_demand.modes import (
    MODE_PREFERENCES_FILES,
    BicyclePreferencesFile,
    BicycleTravelTimesFile,
    ParkAndRidePreferencesFile,
    PublicTransitPreferencesFile,
    WalkingPreferencesFile,
    WalkingTravelTimesFile,
)
from pymetropolis.metro_demand.modes.park_and_ride.files import ParkAndRideStopsFile
from pymetropolis.metro_demand.modes.park_and_ride.transfer_stops import (
    park_and_ride_car_trips,
    park_and_ride_leg_id,
)
from pymetropolis.metro_demand.population import TripsFile
from pymetropolis.metro_demand.population.files import (
    HouseholdsFile,
    JointToursFile,
    PersonsFile,
    ToursModeFile,
)
from pymetropolis.metro_demand.routing.files import (
    NonPrimaryCarTrips,
    NonPrimaryParkAndRideCarTrips,
    ParkAndRideTripsPublicTransitItinerariesFile,
    PrimaryCarTripsAccessEgressFile,
    PrimaryParkAndRideCarTripsAccessEgressFile,
    TripsPublicTransitItinerariesFile,
)
from pymetropolis.metro_environment.fuel import CarFuelFile, ParkAndRideFuelFile
from pymetropolis.metro_pipeline import PopulationStep, Step
from pymetropolis.metro_pipeline.parameters import DurationParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_simulation.common import (
    StepWithRidesharingCount,
    StepWithRidesharingSubsidy,
    merge_populations,
)
from pymetropolis.modes import CAR_MODES, CarMode, ParkAndRide, StepWithModes

from .files import (
    MetroExAnteTripsFile,
    MetroExAnteTripsPopulationFile,
    MetroTripsFile,
    MetroTripsPopulationFile,
)

if TYPE_CHECKING:
    import polars as pl

    from pymetropolis.metro_pipeline.file import MetroDataFrameFile


def clean_trips(trips: pl.DataFrame) -> pl.DataFrame:
    import polars as pl

    if "has_car" not in trips.columns:
        trips = trips.with_columns(has_car=True)
    if "has_driving_license" not in trips.columns:
        trips = trips.with_columns(has_driving_license=True)
    if "destination_activity_duration" not in trips.columns:
        trips = trips.with_columns(destination_activity_duration=pl.lit(None, dtype=pl.Duration))
    if "joint_tour" not in trips.columns:
        trips = trips.with_columns(joint_tour=False)
    trips = trips.with_columns(
        "trip_id",
        agent_id="tour_id",
        activity_time=pl_duration_to_seconds("destination_activity_duration").fill_null(0.0),
    ).sort("agent_id", "trip_id")
    # Set activity time to 0 for the last trip of the tour.
    trips = trips.with_columns(
        activity_time=pl.when(pl.col("trip_id") != pl.col("trip_id").last().over("agent_id"))
        .then("activity_time")
        .otherwise(0.0)
    )
    return trips.select(
        "trip_id", "agent_id", "activity_time", "has_car", "has_driving_license", "joint_tour"
    )


def add_ridesharing_subsidy(df: pl.DataFrame, mode: CarMode, subsidy: float) -> pl.DataFrame:
    """Adds the ridesharing subsidy to the utility of each trip, for modes that involve sharing a
    car with someone else.
    """
    import polars as pl

    if subsidy == 0.0 or not mode.vehicle().has_passenger():
        return df
    if "constant_utility" not in df.columns:
        # Create the `constant_utility` column if it does not exist yet.
        df = df.with_columns(constant_utility=0.0)
    return df.with_columns(constant_utility=pl.col("constant_utility").fill_null(0.0) + subsidy)


@error_context(msg="Cannot generate car trips")
def generate_car_trips(
    mode: CarMode,
    df: pl.DataFrame,
    primary_trips_file: PrimaryCarTripsAccessEgressFile,
    secondary_trips_file: NonPrimaryCarTrips,
    pref_file: MetroDataFrameFile | None = None,
    tstars_file: TstarsFile | None = None,
    schedule_pref_file: LinearScheduleFile | None = None,
    fuel_file: CarFuelFile | None = None,
    fuel_share: float | None = None,
    subsidy: float = 0.0,
):
    import polars as pl

    # Some car modes are restricted to owners of cars.
    if mode.requires_car():
        df = df.filter("has_car")
    # Car-driver modes are only accessible to driving license holders.
    if mode.requires_driving_license():
        df = df.filter("has_driving_license")
    # Car modes without passenger are not feasible for joint tours.
    if not mode.vehicle().has_passenger():
        df = df.filter(pl.col("joint_tour").not_())
    df = df.with_columns(pl.lit(repr(mode)).alias("alt_id"))
    primary_trips: pl.DataFrame = primary_trips_file.read().select(
        "trip_id",
        pl.col("access_node").cast(pl.String).alias("class.origin"),
        pl.col("egress_node").cast(pl.String).alias("class.destination"),
        "access_time",
        "egress_time",
        pl.lit(repr(mode.vehicle())).alias("class.vehicle"),
    )
    df = df.join(primary_trips, on="trip_id", how="left")
    secondary_trips: pl.DataFrame = secondary_trips_file.read().select(
        "trip_id", pl_duration_to_seconds("free_flow_travel_time").alias("class.travel_time")
    )
    df = df.join(secondary_trips, on="trip_id", how="left")
    df = df.with_columns(
        pl.when(pl.col("class.origin").is_not_null())
        .then(pl.lit("Road"))
        .when(pl.col("class.travel_time").is_not_null())
        .then(pl.lit("Virtual"))
        .alias("class.type"),
        access_time_sec=pl_duration_to_seconds("access_time").fill_null(0.0),
        egress_time_sec=pl_duration_to_seconds("egress_time").fill_null(0.0),
    ).drop("access_time", "egress_time")
    # Drop trips which are neither primary nor secondary.
    df = df.filter(pl.col("class.type").is_not_null())
    # Compute stopping time at destination: egress time + activity time + next access time.
    df = df.with_columns(
        stopping_time=pl.col("egress_time_sec")
        + pl.col("activity_time")
        + pl.col("access_time_sec").shift(-1).over("agent_id").fill_null(0.0)
    )
    if pref_file is not None and pref_file.exists():
        # The mode constant is defined at the tour level so it is only added at the alternative
        # level.
        params: pl.DataFrame = pref_file.read().select(
            agent_id="tour_id", alpha=pl.col(f"{mode!r}_vot") / 3600.0
        )
        df = df.join(params, on="agent_id", how="left")
        # Decrease utility by the access and egress time's value of time.
        df = df.with_columns(
            constant_utility=-pl.col("alpha")
            * (pl.col("access_time_sec") + pl.col("egress_time_sec"))
        )
    df = add_schedule_preferences(df, schedule_pref_file, tstars_file)
    if "schedule_utility.tstar" in df.columns:
        # Add egress time to tstar.
        df = df.with_columns(pl.col("schedule_utility.tstar") - pl.col("egress_time_sec"))
    if fuel_file is not None and fuel_file.exists() and fuel_share != 0.0:
        fuel: pl.DataFrame = fuel_file.read()
        if "constant_utility" not in df.columns:
            # Create the `constant_utility` column if it does not exist yet.
            df = df.with_columns(constant_utility=0.0)
        # Subtract the fuel cost paid from the constant utility.
        df = (
            df.join(fuel.select("trip_id", "fuel_cost"), on="trip_id", how="left")
            .with_columns(
                constant_utility=pl.col("constant_utility").fill_null(0.0)
                - pl.col("fuel_cost") * fuel_share
            )
            .drop("fuel_cost")
        )
    df = add_ridesharing_subsidy(df, mode, subsidy)
    df = df.drop("activity_time", "access_time_sec", "egress_time_sec")
    return df


@error_context(msg="Cannot generate park-and-ride trips")
def generate_park_and_ride_trips(
    df: pl.DataFrame,
    trips: pl.DataFrame,
    pr_stops_file: ParkAndRideStopsFile,
    primary_trips_file: PrimaryParkAndRideCarTripsAccessEgressFile,
    secondary_trips_file: NonPrimaryParkAndRideCarTrips,
    park_and_ride_itineraries_file: ParkAndRideTripsPublicTransitItinerariesFile,
    pt_itineraries_file: TripsPublicTransitItinerariesFile,
    pref_file: ParkAndRidePreferencesFile | None = None,
    tstars_file: TstarsFile | None = None,
    schedule_pref_file: LinearScheduleFile | None = None,
    fuel_file: ParkAndRideFuelFile | None = None,
    transfer_time: float = 0.0,
):
    """Generates the trips of the park-and-ride alternatives.

    Each trip of the P+R tours is split into one or two Metropolis-Core trips (legs):

    - first trip of the tour (outbound): car leg from the origin to the P+R facility, then
      public-transit leg from the P+R facility to the destination;
    - last trip of the tour (inbound): public-transit leg from the origin to the P+R facility, then
      car leg from the P+R facility to the destination;
    - intermediary trips: single public-transit leg (same as the `public_transit` mode).

    The legs of a split trip have ids `{trip_id}-1` and `{trip_id}-2`.
    The `transfer_time` (in seconds) is spent at the P+R facility, between the two legs.
    The schedule-delay preferences of a trip apply to its last leg.
    The car legs use the `car_driver_alone` vehicle.
    """
    import polars as pl
    import polars.selectors as cs

    if df["trip_id"].dtype != pl.String:
        raise MetropyError("Park-and-ride requires trip ids to be strings")
    # P+R requires a car and a driving license, and the car part has no passenger so it is not
    # feasible for joint tours.
    df = df.filter("has_car", "has_driving_license", pl.col("joint_tour").not_())
    tour_ids = pr_stops_file.read_as_df()["tour_id"]
    df = df.filter(pl.col("agent_id").is_in(tour_ids.implode()))
    car_trips = park_and_ride_car_trips(trips, tour_ids).select("trip_id", "is_outbound")
    # `is_outbound` is null for the intermediary trips (without car leg).
    df = df.join(car_trips, on="trip_id", how="left").with_columns(
        pl.lit(repr(ParkAndRide)).alias("alt_id")
    )

    # Public-transit legs: specific itineraries for the first / last trips, standard itineraries
    # for the intermediary trips.
    def read_itineraries(itineraries_file, suffix: str) -> pl.DataFrame:
        itineraries: pl.DataFrame = itineraries_file.read()
        if "generalized_time" not in itineraries.columns:
            itineraries = itineraries.with_columns(generalized_time="travel_time")
        return itineraries.select(
            "trip_id",
            pl_duration_to_seconds("travel_time").alias(f"pt_travel_time{suffix}"),
            pl_duration_to_seconds("generalized_time")
            .fill_null(pl_duration_to_seconds("travel_time"))
            .alias(f"pt_generalized_time{suffix}"),
        )

    df = (
        df.join(read_itineraries(park_and_ride_itineraries_file, "_pr"), on="trip_id", how="left")
        .join(read_itineraries(pt_itineraries_file, "_std"), on="trip_id", how="left")
        .with_columns(
            pt_travel_time=pl.when(pl.col("is_outbound").is_null())
            .then("pt_travel_time_std")
            .otherwise("pt_travel_time_pr"),
            pt_generalized_time=pl.when(pl.col("is_outbound").is_null())
            .then("pt_generalized_time_std")
            .otherwise("pt_generalized_time_pr"),
        )
        .drop(cs.ends_with("_std", "_pr"))
    )

    # Car legs.
    primary_trips: pl.DataFrame = primary_trips_file.read().select(
        "trip_id",
        pl.col("access_node").cast(pl.String).alias("car_origin"),
        pl.col("egress_node").cast(pl.String).alias("car_destination"),
        pl_duration_to_seconds("access_time").alias("access_time_sec"),
        pl_duration_to_seconds("egress_time").alias("egress_time_sec"),
    )
    secondary_trips: pl.DataFrame = secondary_trips_file.read().select(
        "trip_id", pl_duration_to_seconds("free_flow_travel_time").alias("car_virtual_time")
    )
    df = (
        df.join(primary_trips, on="trip_id", how="left")
        .join(secondary_trips, on="trip_id", how="left")
        .with_columns(
            pl.col("access_time_sec", "egress_time_sec").fill_null(0.0),
            car_type=pl.when(pl.col("car_origin").is_not_null())
            .then(pl.lit("Road"))
            .when(pl.col("car_virtual_time").is_not_null())
            .then(pl.lit("Virtual")),
        )
    )
    # Discard the tours with an infeasible leg.
    is_feasible = pl.col("pt_travel_time").is_not_null() & (
        pl.col("is_outbound").is_null() | pl.col("car_type").is_not_null()
    )
    df = df.filter(is_feasible.all().over("agent_id"))

    # Utility.
    has_prefs = pref_file is not None and pref_file.exists()
    if has_prefs:
        assert pref_file is not None
        # The mode constant is defined at the tour level so it is only added at the alternative
        # level.
        params: pl.DataFrame = pref_file.read().select(
            agent_id="tour_id",
            car_alpha=pl.col("car_vot") / 3600.0,
            pt_alpha=pl.col("public_transit_vot") / 3600.0,
        )
        df = df.join(params, on="agent_id", how="left").with_columns(
            pl.col("car_alpha", "pt_alpha").fill_null(0.0)
        )
    else:
        df = df.with_columns(car_alpha=pl.lit(0.0), pt_alpha=pl.lit(0.0))
    df = df.with_columns(
        # Value of the generalized time of the public-transit leg and of the transfer time.
        pt_utility=-pl.col("pt_alpha")
        * (
            pl.col("pt_generalized_time")
            + pl.when(pl.col("is_outbound").is_not_null()).then(transfer_time).otherwise(0.0)
        ),
        # Value of the access / egress time of the car leg.
        car_utility=-pl.col("car_alpha") * (pl.col("access_time_sec") + pl.col("egress_time_sec")),
    )
    if fuel_file is not None and fuel_file.exists():
        # The P+R traveler drives alone and pays the full fuel cost.
        fuel: pl.DataFrame = fuel_file.read().select("trip_id", "fuel_cost")
        df = (
            df.join(fuel, on="trip_id", how="left")
            .with_columns(car_utility=pl.col("car_utility") - pl.col("fuel_cost").fill_null(0.0))
            .drop("fuel_cost")
        )

    # Schedule-delay preferences (for the last leg of each trip).
    df = add_schedule_preferences(df, schedule_pref_file, tstars_file)
    if "schedule_utility.tstar" in df.columns:
        # For inbound trips, the last leg is the car leg: add egress time to tstar.
        df = df.with_columns(
            pl.when(pl.col("is_outbound").eq(False))
            .then(pl.col("schedule_utility.tstar") - pl.col("egress_time_sec"))
            .otherwise("schedule_utility.tstar")
        )
    schedule_cols = [c for c in df.columns if c.startswith("schedule_utility.")]

    common_cols = ["agent_id", "alt_id", "has_car", "has_driving_license"]
    car_leg = df.filter(pl.col("is_outbound").is_not_null()).select(
        *common_cols,
        park_and_ride_leg_id(pl.col("trip_id"), pl.when("is_outbound").then(1).otherwise(2)).alias(
            "trip_id"
        ),
        pl.col("car_type").alias("class.type"),
        pl.col("car_origin").alias("class.origin"),
        pl.col("car_destination").alias("class.destination"),
        pl.when(pl.col("car_type") == "Road")
        .then(pl.lit(repr(ParkAndRide.vehicle())))
        .alias("class.vehicle"),
        pl.col("car_virtual_time").alias("class.travel_time"),
        # Outbound: egress time + transfer time at the P+R facility.
        # Inbound: egress time + activity time at destination.
        stopping_time=pl.col("egress_time_sec")
        + pl.when("is_outbound").then(transfer_time).otherwise("activity_time"),
        alpha="car_alpha",
        constant_utility="car_utility",
        *[pl.when(pl.col("is_outbound").not_()).then(c).alias(c) for c in schedule_cols],
    )
    pt_leg = df.select(
        *common_cols,
        pl.when(pl.col("is_outbound").is_null())
        .then("trip_id")
        .otherwise(
            park_and_ride_leg_id(pl.col("trip_id"), pl.when("is_outbound").then(2).otherwise(1))
        )
        .alias("trip_id"),
        pl.lit("Virtual").alias("class.type"),
        pl.col("pt_travel_time").alias("class.travel_time"),
        # Inbound: transfer time + access time of the car leg.
        # Otherwise: activity time at destination.
        stopping_time=pl.when(pl.col("is_outbound").eq(False))
        .then(transfer_time + pl.col("access_time_sec"))
        .otherwise("activity_time"),
        constant_utility="pt_utility",
        *[
            pl.when(pl.col("is_outbound").eq(False)).then(None).otherwise(c).alias(c)
            for c in schedule_cols
        ],
    )
    df = pl.concat((car_leg, pt_leg), how="diagonal").sort("agent_id", "trip_id")
    if not has_prefs:
        df = df.drop("alpha", "constant_utility")
    return df


@error_context(msg="Cannot generate public-transit trips")
def generate_public_transit_trips(
    df: pl.DataFrame,
    itineraries_file: TripsPublicTransitItinerariesFile,
    pref_file: PublicTransitPreferencesFile | None = None,
    tstars_file: TstarsFile | None = None,
    schedule_pref_file: LinearScheduleFile | None = None,
):
    import polars as pl

    df = df.with_columns(
        pl.lit("public_transit").alias("alt_id"), pl.lit("Virtual").alias("class.type")
    ).rename({"activity_time": "stopping_time"})
    itineraries: pl.DataFrame = itineraries_file.read()
    df = df.join(
        itineraries.select(
            "trip_id", pl_duration_to_seconds("travel_time").alias("class.travel_time")
        ),
        on="trip_id",
        how="inner",
    )
    df = df.filter(pl.col("class.travel_time").is_not_null().all().over("agent_id"))
    if pref_file is not None and pref_file.exists():
        params: pl.DataFrame = pref_file.read().select(agent_id="tour_id", vot="public_transit_vot")
        if "generalized_time" not in itineraries.columns:
            itineraries = itineraries.with_columns(generalized_time="travel_time")
        itineraries = itineraries.with_columns(
            generalized_time=pl_duration_to_seconds("generalized_time").fill_null(
                pl_duration_to_seconds("travel_time")
            )
        )
        # Set the utility equal to minus the value of time * generalized time.
        # This allows to consider different values of time for different modes (walking, waiting,
        # bus, subway, etc.).
        # The mode constant is defined at the tour level so it is added to the utility of the
        # alternative, not of the trips.
        df = (
            df.join(params, on="agent_id", how="left")
            .join(itineraries.select("trip_id", "generalized_time"), on="trip_id", how="left")
            .with_columns(constant_utility=-pl.col("vot") * pl.col("generalized_time") / 3600)
            .drop("vot", "generalized_time")
        )
    df = add_schedule_preferences(df, schedule_pref_file, tstars_file)
    return df


@error_context(msg="Cannot generate walking trips")
def generate_walking_trips(
    df: pl.DataFrame,
    tts_file: WalkingTravelTimesFile,
    pref_file: WalkingPreferencesFile | None = None,
    tstars_file: TstarsFile | None = None,
    schedule_pref_file: LinearScheduleFile | None = None,
):
    import polars as pl

    df = df.with_columns(
        pl.lit("walking").alias("alt_id"), pl.lit("Virtual").alias("class.type")
    ).rename({"activity_time": "stopping_time"})
    tts: pl.DataFrame = tts_file.read()
    df = (
        df.join(tts, on="trip_id", how="left")
        .with_columns(pl_duration_to_seconds("walking_travel_time").alias("class.travel_time"))
        .drop("walking_travel_time")
    )
    if pref_file is not None and pref_file.exists():
        # The mode constant is defined at the tour level so it is added to the utility of the
        # alternative, not of the trips.
        params: pl.DataFrame = pref_file.read().select(
            agent_id="tour_id", alpha=pl.col("walking_vot") / 3600.0
        )
        df = df.join(params, on="agent_id", how="left")
    df = add_schedule_preferences(df, schedule_pref_file, tstars_file)
    return df


@error_context(msg="Cannot generate bicycle trips")
def generate_bicycle_trips(
    df: pl.DataFrame,
    tts_file: BicycleTravelTimesFile,
    pref_file: BicyclePreferencesFile | None = None,
    tstars_file: TstarsFile | None = None,
    schedule_pref_file: LinearScheduleFile | None = None,
):
    import polars as pl

    df = df.with_columns(
        pl.lit("bicycle").alias("alt_id"), pl.lit("Virtual").alias("class.type")
    ).rename({"activity_time": "stopping_time"})
    tts: pl.DataFrame = tts_file.read()
    df = (
        df.join(tts, on="trip_id", how="left")
        .with_columns(pl_duration_to_seconds("bicycle_travel_time").alias("class.travel_time"))
        .drop("bicycle_travel_time")
    )
    if pref_file is not None and pref_file.exists():
        # The mode constant is defined at the tour level so it is added to the utility of the
        # alternative, not of the trips.
        params: pl.DataFrame = pref_file.read().select(
            agent_id="tour_id", alpha=pl.col("bicycle_vot") / 3600.0
        )
        df = df.join(params, on="agent_id", how="left")
    df = add_schedule_preferences(df, schedule_pref_file, tstars_file)
    return df


def add_schedule_preferences(
    df: pl.DataFrame, schedule_pref_file: LinearScheduleFile | None, tstars_file: TstarsFile | None
) -> pl.DataFrame:
    import polars as pl

    if tstars_file is not None and tstars_file.exists():
        df = (
            df.join(tstars_file.read(), on="trip_id", how="left")
            .with_columns(pl_duration_to_seconds("tstar").alias("schedule_utility.tstar"))
            .drop("tstar")
        )
    if schedule_pref_file is not None and schedule_pref_file.exists():
        if "schedule_utility.tstar" not in df.columns:
            logger.warning("Schedule-delay parameters are defined but there is no tstar.")
        df = (
            df.join(schedule_pref_file.read(), on="trip_id", how="left")
            .with_columns(
                pl.lit("Linear").alias("schedule_utility.type"),
                (pl.col("beta") / 3600.0).alias("schedule_utility.beta"),
                (pl.col("gamma") / 3600.0).alias("schedule_utility.gamma"),
                pl_duration_to_seconds("delta").alias("schedule_utility.delta"),
            )
            .drop("beta", "gamma", "delta")
        )
    return df


class PrepareMetroTripsStep(
    StepWithModes, StepWithRidesharingCount, StepWithRidesharingSubsidy, PopulationStep
):
    """Prepares the trips for the Metropolis-Core simulation.

    For the `park_and_ride` mode, the first and last trips of each tour are split into a car leg and
    a public-transit leg, with ids `{trip_id}-1` and `{trip_id}-2`, separated by
    [`modes.park_and_ride.transfer_time`](parameters.md#modespark_and_ridetransfer_time) (see
    [`ParkAndRideFacilitiesFromNearestStopStep`](steps.md#parkandridefacilitiesfromneareststopstep)
    for the definition of the P+R facilities).
    Intermediary trips are traveled by public transit.
    The car legs use the `car_driver_alone` vehicle and are valued with the `car_driver` value of
    time, while the public-transit legs and the transfer time are valued with the `public_transit`
    value of time.
    Schedule-delay preferences apply to the last leg of each trip.
    """

    input_files = {
        "trips": TripsFile,
        "persons": InputFile(PersonsFile, optional=True),
        "households": InputFile(HouseholdsFile, optional=True),
        "primary_car_trips": InputFile(
            PrimaryCarTripsAccessEgressFile,
            when=lambda inst: inst.has_car_mode(),
            when_doc=r'if any "car\_\*" mode is defined',
        ),
        "secondary_car_trips": InputFile(
            NonPrimaryCarTrips,
            when=lambda inst: inst.has_car_mode(),
            when_doc=r'if any "car\_\*" mode is defined',
        ),
        "public_transit_itineraries": InputFile(
            TripsPublicTransitItinerariesFile,
            when=lambda inst: inst.has_mode("public_transit") or inst.has_mode("park_and_ride"),
            when_doc='if the "public_transit" or "park_and_ride" mode is defined',
        ),
        "walking_travel_times": InputFile(
            WalkingTravelTimesFile,
            when=lambda inst: inst.has_mode("walking"),
            when_doc='if the "walking" mode is defined',
        ),
        "bicycle_travel_times": InputFile(
            BicycleTravelTimesFile,
            when=lambda inst: inst.has_mode("bicycle"),
            when_doc='if the "bicycle" mode is defined',
        ),
        "linear_schedule": InputFile(LinearScheduleFile, optional=True),
        "tstars": InputFile(TstarsFile, optional=True),
        "car_fuel": InputFile(
            CarFuelFile,
            optional=True,
            when=lambda inst: inst.has_car_mode(),
            when_doc=r'if any "car\_\*" mode is defined',
        ),
        "pr_stops": InputFile(
            ParkAndRideStopsFile,
            when=lambda inst: inst.has_mode("park_and_ride"),
            when_doc='if the "park_and_ride" mode is defined',
        ),
        "primary_pr_trips": InputFile(
            PrimaryParkAndRideCarTripsAccessEgressFile,
            when=lambda inst: inst.has_mode("park_and_ride"),
            when_doc='if the "park_and_ride" mode is defined',
        ),
        "secondary_pr_trips": InputFile(
            NonPrimaryParkAndRideCarTrips,
            when=lambda inst: inst.has_mode("park_and_ride"),
            when_doc='if the "park_and_ride" mode is defined',
        ),
        "pr_pt_itineraries": InputFile(
            ParkAndRideTripsPublicTransitItinerariesFile,
            when=lambda inst: inst.has_mode("park_and_ride"),
            when_doc='if the "park_and_ride" mode is defined',
        ),
        "pr_fuel": InputFile(
            ParkAndRideFuelFile,
            optional=True,
            when=lambda inst: inst.has_mode("park_and_ride"),
            when_doc='if the "park_and_ride" mode is defined',
        ),
        "joint_tours": InputFile(JointToursFile, optional=True),
        **{
            f"{mode!r}_preferences": InputFile(
                pref_file,
                optional=True,
                when=lambda inst, mode=mode: inst.has_mode_class(mode),
                when_doc=f'if the "{mode!r}" mode is defined',
            )
            for mode, pref_file in MODE_PREFERENCES_FILES.items()
        },
    }
    output_files = {"metro_trips": MetroTripsPopulationFile}

    pr_transfer_time = DurationParameter(
        "modes.park_and_ride.transfer_time",
        default=timedelta(minutes=5),
        description=(
            "Time spent at the P+R facility between the car part and the public-transit part of "
            "park-and-ride trips (parking, walking to the platform)."
        ),
        note="This time is valued at the public-transit value of time.",
    )

    def is_defined(self) -> bool:
        if not self.has_any_mode():
            return False
        # If there is no "trip mode", this step cannot be run (there is no trip to generate).
        return self.has_trip_mode()

    def run(self):
        import polars as pl

        assert self.ridesharing_passenger_count is not None

        trips = self.input["trips"].read()
        if self.input["persons"].exists():
            persons = self.input["persons"].read()
            if "has_driving_license" in persons.columns:
                trips = trips.join(
                    persons.select("person_id", pl.col("has_driving_license").fill_null(False)),
                    on="person_id",
                    how="left",
                )
        if self.input["households"].exists():
            households = self.input["households"].read()
            if "nb_cars" in households.columns:
                trips = trips.join(
                    households.select(
                        "household_id", has_car=pl.col("nb_cars").gt(0).fill_null(False)
                    ),
                    on="household_id",
                    how="left",
                )
        if self.input["joint_tours"].exists():
            joint_tours = self.input["joint_tours"].read()
            trips = trips.join(joint_tours, on="tour_id", how="left")
        df = clean_trips(trips)
        metro_trips = pl.DataFrame()
        for car_mode in CAR_MODES:
            if self.has_mode_class(car_mode):
                fuel_share = car_mode.get_fuel_share(self.ridesharing_passenger_count)
                car_trips = generate_car_trips(
                    car_mode,
                    df=df,
                    primary_trips_file=self.input["primary_car_trips"],
                    secondary_trips_file=self.input["secondary_car_trips"],
                    pref_file=self.input[f"{car_mode!r}_preferences"],
                    tstars_file=self.input["tstars"],
                    schedule_pref_file=self.input["linear_schedule"],
                    fuel_file=self.input["car_fuel"],
                    fuel_share=fuel_share,
                    subsidy=self.ridesharing_subsidy,
                )
                metro_trips = pl.concat((metro_trips, car_trips), how="diagonal")
        if self.has_mode("park_and_ride"):
            assert self.pr_transfer_time is not None
            park_and_ride_trips = generate_park_and_ride_trips(
                df,
                trips=trips,
                pr_stops_file=self.input["pr_stops"],
                primary_trips_file=self.input["primary_pr_trips"],
                secondary_trips_file=self.input["secondary_pr_trips"],
                park_and_ride_itineraries_file=self.input["pr_pt_itineraries"],
                pt_itineraries_file=self.input["public_transit_itineraries"],
                pref_file=self.input["park_and_ride_preferences"],
                tstars_file=self.input["tstars"],
                schedule_pref_file=self.input["linear_schedule"],
                fuel_file=self.input["pr_fuel"],
                transfer_time=self.pr_transfer_time.total_seconds(),
            )
            metro_trips = pl.concat((metro_trips, park_and_ride_trips), how="diagonal")
        if self.has_mode("public_transit"):
            public_transit_trips = generate_public_transit_trips(
                df,
                self.input["public_transit_itineraries"],
                self.input["public_transit_preferences"],
                self.input["tstars"],
                self.input["linear_schedule"],
            )
            metro_trips = pl.concat((metro_trips, public_transit_trips), how="diagonal")
        if self.has_mode("walking"):
            walking_trips = generate_walking_trips(
                df,
                self.input["walking_travel_times"],
                self.input["walking_preferences"],
                self.input["tstars"],
                self.input["linear_schedule"],
            )
            metro_trips = pl.concat((metro_trips, walking_trips), how="diagonal")
        if self.has_mode("bicycle"):
            bicycle_trips = generate_bicycle_trips(
                df,
                self.input["bicycle_travel_times"],
                self.input["bicycle_preferences"],
                self.input["tstars"],
                self.input["linear_schedule"],
            )
            metro_trips = pl.concat((metro_trips, bicycle_trips), how="diagonal")
        metro_trips = metro_trips.drop(
            "has_car", "has_driving_license", "joint_tour", strict=False
        ).sort("agent_id", "alt_id", "trip_id")
        self.output["metro_trips"].write(metro_trips)


class PrepareExAnteMetroTripsStep(StepWithModes, StepWithRidesharingCount, PopulationStep):
    """Prepares the trips using ex-ante modes and departure time for the ex-ante simulation.

    Preference parameters are not defined (they have no impact on route choice).
    """

    input_files = {
        "trips": TripsFile,
        "tour_modes": ToursModeFile,
        "primary_car_trips": InputFile(
            PrimaryCarTripsAccessEgressFile,
            when=lambda inst: inst.has_car_mode(),
            when_doc=r'if any "car\_\*" mode is defined',
        ),
        "secondary_car_trips": InputFile(
            NonPrimaryCarTrips,
            when=lambda inst: inst.has_car_mode(),
            when_doc=r'if any "car\_\*" mode is defined',
        ),
        "public_transit_travel_times": InputFile(
            TripsPublicTransitItinerariesFile,
            when=lambda inst: inst.has_mode("public_transit"),
            when_doc='if the "public_transit" mode is defined',
        ),
        "walking_travel_times": InputFile(
            WalkingTravelTimesFile,
            when=lambda inst: inst.has_mode("walking"),
            when_doc='if the "walking" mode is defined',
        ),
        "bicycle_travel_times": InputFile(
            BicycleTravelTimesFile,
            when=lambda inst: inst.has_mode("bicycle"),
            when_doc='if the "bicycle" mode is defined',
        ),
    }
    output_files = {"metro_trips": MetroExAnteTripsPopulationFile}
    priority = 0

    def is_defined(self) -> bool:
        # If there is no "road-based mode", this step cannot be run (there is no trip to generate).
        return self.has_trip_mode()

    def run(self):
        import polars as pl

        trips: pl.DataFrame = self.input["trips"].read()
        df = clean_trips(trips)
        metro_trips = pl.DataFrame()
        for car_mode in CAR_MODES:
            if self.has_mode_class(car_mode):
                car_trips = generate_car_trips(
                    car_mode,
                    df=df,
                    primary_trips_file=self.input["primary_car_trips"],
                    secondary_trips_file=self.input["secondary_car_trips"],
                )
                metro_trips = pl.concat((metro_trips, car_trips), how="diagonal")
        if self.has_mode("public_transit"):
            public_transit_trips = generate_public_transit_trips(
                df, self.input["public_transit_travel_times"]
            )
            metro_trips = pl.concat((metro_trips, public_transit_trips), how="diagonal")
        if self.has_mode("walking"):
            walking_trips = generate_walking_trips(df, self.input["walking_travel_times"])
            metro_trips = pl.concat((metro_trips, walking_trips), how="diagonal")
        if self.has_mode("bicycle"):
            bicycle_trips = generate_bicycle_trips(df, self.input["bicycle_travel_times"])
            metro_trips = pl.concat((metro_trips, bicycle_trips), how="diagonal")
        metro_trips = metro_trips.sort("agent_id", "alt_id", "trip_id")
        # Keep only trips with the ex-ante mode.
        tour_modes = self.input["tour_modes"].read()
        metro_trips = metro_trips.join(
            tour_modes, left_on=["agent_id", "alt_id"], right_on=["tour_id", "mode"], how="semi"
        )
        # In the ex-ante simulation, 1 agent = 1 trip.
        metro_trips = metro_trips.with_columns(agent_id="trip_id")
        metro_trips = metro_trips.drop("has_car", "has_driving_license").sort(
            "agent_id", "alt_id", "trip_id"
        )
        self.output["metro_trips"].write(metro_trips)


class WriteMetroTripsStep(Step):
    """Merges the trips in each population and writes the trips input file for Metropolis-Core."""

    input_files = {"population_trips": InputFile(MetroTripsPopulationFile, all_populations=True)}
    output_files = {"metro_trips": MetroTripsFile}

    # TODO. There is an issue if a population has no trip defined (e.g., only `outside_option`
    # alternatives) since this Step will never be executed in this case.
    def run(self):
        trips = merge_populations(
            self.input_populations["population_trips"], id_columns=("agent_id", "trip_id")
        )
        self.output["metro_trips"].write(trips)


class WriteExAnteMetroTripsStep(Step):
    """Merges the trips in each population and writes the trips input file for the ex-ante
    simulation.
    """

    input_files = {
        "population_trips": InputFile(MetroExAnteTripsPopulationFile, all_populations=True)
    }
    output_files = {"metro_trips": MetroExAnteTripsFile}
    priority = 0

    def run(self):
        trips = merge_populations(
            self.input_populations["population_trips"], id_columns=("agent_id", "trip_id")
        )
        self.output["metro_trips"].write(trips)
