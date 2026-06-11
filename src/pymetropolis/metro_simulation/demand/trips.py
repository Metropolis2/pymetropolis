from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_common.errors import error_context
from pymetropolis.metro_common.utils import pl_duration_to_seconds
from pymetropolis.metro_demand.departure_time import LinearScheduleFile, TstarsFile
from pymetropolis.metro_demand.modes import PublicTransitPreferencesFile
from pymetropolis.metro_demand.modes.car import (
    CarDriverPreferencesFile,
    CarDriverWithPassengersPreferencesFile,
    CarFuelFile,
    CarPassengerPreferencesFile,
    CarRidesharingPreferencesFile,
)
from pymetropolis.metro_demand.modes.files import (
    BicyclePreferencesFile,
    BicycleTravelTimesFile,
    WalkingPreferencesFile,
    WalkingTravelTimesFile,
)
from pymetropolis.metro_demand.population import TripsFile
from pymetropolis.metro_demand.routing.files import (
    NonPrimaryCarTrips,
    PrimaryCarTripsAccessEgressFile,
    TripsPublicTransitItinerariesFile,
)
from pymetropolis.metro_pipeline.file import MetroDataFrameFile
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_simulation.common import StepWithModes, StepWithRidesharingCount

from .files import MetroTripsFile

if TYPE_CHECKING:
    import polars as pl


@error_context(msg="Cannot generate car trips")
def generate_car_trips(
    mode: str,
    vehicle_type: str,
    df: pl.DataFrame,
    primary_trips_file: PrimaryCarTripsAccessEgressFile,
    secondary_trips_file: NonPrimaryCarTrips,
    pref_file: MetroDataFrameFile,
    tstars_file: TstarsFile,
    schedule_pref_file: LinearScheduleFile,
    fuel_file: CarFuelFile,
    fuel_share: float,
):
    import polars as pl

    df = df.with_columns(pl.lit(mode).alias("alt_id"))
    primary_trips: pl.DataFrame = primary_trips_file.read().select(
        "trip_id",
        pl.col("access_node").cast(pl.String).alias("class.origin"),
        pl.col("egress_node").cast(pl.String).alias("class.destination"),
        "access_time",
        "egress_time",
        pl.lit(vehicle_type).alias("class.vehicle"),
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
    if pref_file.exists():
        params: pl.DataFrame = pref_file.read().select(
            "person_id",
            constant_utility=-pl.col(f"{mode}_cst"),
            alpha=pl.col(f"{mode}_vot") / 3600.0,
        )
        df = df.join(params, on="person_id", how="left")
        # Decrease utility by the access and egress time's value of time.
        df = df.with_columns(
            constant_utility=pl.col("constant_utility")
            - pl.col("alpha") * (pl.col("access_time_sec") + pl.col("egress_time_sec"))
        )
    if tstars_file.exists():
        tstars: pl.DataFrame = tstars_file.read()
        df = (
            df.join(tstars, on="trip_id", how="left")
            .with_columns(pl_duration_to_seconds("tstar").alias("schedule_utility.tstar"))
            .drop("tstar")
        )
    if schedule_pref_file.exists():
        params: pl.DataFrame = schedule_pref_file.read()
        # TODO: Send warning if linear schedule is defined but not tstars.
        df = (
            df.join(params, on="trip_id", how="left")
            .with_columns(
                pl.lit("Linear").alias("schedule_utility.type"),
                (pl.col("beta") / 3600.0).alias("schedule_utility.beta"),
                (pl.col("gamma") / 3600.0).alias("schedule_utility.gamma"),
                pl_duration_to_seconds("delta").alias("schedule_utility.delta"),
            )
            .drop("beta", "gamma", "delta")
        )
    if fuel_file.exists() and fuel_share != 0.0:
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
    df = df.drop("activity_time", "access_time_sec", "egress_time_sec")
    return df


@error_context(msg="Cannot generate public-transit trips")
def generate_public_transit_trips(
    df: pl.DataFrame,
    itineraries_file: TripsPublicTransitItinerariesFile,
    pref_file: PublicTransitPreferencesFile,
    tstars_file: TstarsFile,
    schedule_pref_file: LinearScheduleFile,
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
    if pref_file.exists():
        params: pl.DataFrame = pref_file.read()
        if "generalized_time" not in itineraries.columns:
            itineraries = itineraries.with_columns(generalized_time="travel_time")
        itineraries = itineraries.with_columns(
            generalized_time=pl_duration_to_seconds("generalized_time").fill_null(
                pl_duration_to_seconds("travel_time")
            )
        )
        # Set the utility equal to the -constant - value of time * generalized time.
        # This allows to consider different values of time for different modes (walking, waiting,
        # bus, subway, etc.).
        df = (
            df.join(params, on="person_id", how="left")
            .join(itineraries.select("trip_id", "generalized_time"), on="trip_id", how="left")
            .with_columns(
                constant_utility=-pl.col("public_transit_cst")
                - pl.col("public_transit_vot") * pl.col("generalized_time") / 3600
            )
            .drop("public_transit_cst", "public_transit_vot", "generalized_time")
        )
    if tstars_file.exists():
        tstars: pl.DataFrame = tstars_file.read()
        df = (
            df.join(tstars, on="trip_id", how="left")
            .with_columns(pl_duration_to_seconds("tstar").alias("schedule_utility.tstar"))
            .drop("tstar")
        )
    if schedule_pref_file.exists():
        params: pl.DataFrame = schedule_pref_file.read()
        df = (
            df.join(params, on="trip_id", how="left")
            .with_columns(
                pl.lit("Linear").alias("schedule_utility.type"),
                (pl.col("beta") / 3600.0).alias("schedule_utility.beta"),
                (pl.col("gamma") / 3600.0).alias("schedule_utility.gamma"),
                pl_duration_to_seconds("delta").alias("schedule_utility.delta"),
            )
            .drop("beta", "gamma", "delta")
        )
    return df


@error_context(msg="Cannot generate walking trips")
def generate_walking_trips(
    df: pl.DataFrame,
    tts_file: WalkingTravelTimesFile,
    pref_file: WalkingPreferencesFile,
    tstars_file: TstarsFile,
    schedule_pref_file: LinearScheduleFile,
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
    if pref_file.exists():
        params: pl.DataFrame = pref_file.read()
        df = (
            df.join(params, on="person_id", how="left")
            .with_columns(
                constant_utility=-pl.col("walking_cst"), alpha=pl.col("walking_vot") / 3600.0
            )
            .drop("walking_cst", "walking_vot")
        )
    if tstars_file.exists():
        tstars: pl.DataFrame = tstars_file.read()
        df = (
            df.join(tstars, on="trip_id", how="left")
            .with_columns(pl_duration_to_seconds("tstar").alias("schedule_utility.tstar"))
            .drop("tstar")
        )
    if schedule_pref_file.exists():
        params: pl.DataFrame = schedule_pref_file.read()
        df = (
            df.join(params, on="trip_id", how="left")
            .with_columns(
                pl.lit("Linear").alias("schedule_utility.type"),
                (pl.col("beta") / 3600.0).alias("schedule_utility.beta"),
                (pl.col("gamma") / 3600.0).alias("schedule_utility.gamma"),
                pl_duration_to_seconds("delta").alias("schedule_utility.delta"),
            )
            .drop("beta", "gamma", "delta")
        )
    return df


@error_context(msg="Cannot generate bicycle trips")
def generate_bicycle_trips(
    df: pl.DataFrame,
    tts_file: BicycleTravelTimesFile,
    pref_file: BicyclePreferencesFile,
    tstars_file: TstarsFile,
    schedule_pref_file: LinearScheduleFile,
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
    if pref_file.exists():
        params: pl.DataFrame = pref_file.read()
        df = (
            df.join(params, on="person_id", how="left")
            .with_columns(
                constant_utility=-pl.col("bicycle_cst"), alpha=pl.col("bicycle_vot") / 3600.0
            )
            .drop("bicycle_cst", "bicycle_vot")
        )
    if tstars_file.exists():
        tstars: pl.DataFrame = tstars_file.read()
        df = (
            df.join(tstars, on="trip_id", how="left")
            .with_columns(pl_duration_to_seconds("tstar").alias("schedule_utility.tstar"))
            .drop("tstar")
        )
    if schedule_pref_file.exists():
        params: pl.DataFrame = schedule_pref_file.read()
        df = (
            df.join(params, on="trip_id", how="left")
            .with_columns(
                pl.lit("Linear").alias("schedule_utility.type"),
                (pl.col("beta") / 3600.0).alias("schedule_utility.beta"),
                (pl.col("gamma") / 3600.0).alias("schedule_utility.gamma"),
                pl_duration_to_seconds("delta").alias("schedule_utility.delta"),
            )
            .drop("beta", "gamma", "delta")
        )
    return df


class WriteMetroTripsStep(StepWithModes, StepWithRidesharingCount):
    """Generates the input trips file for the Metropolis-Core simulation."""

    input_files = {
        "trips": TripsFile,
        "primary_car_trips": InputFile(
            PrimaryCarTripsAccessEgressFile,
            when=lambda inst: inst.has_car_mode(),
            when_doc='if any "car_*" mode is defined',
        ),
        "secondary_car_trips": InputFile(
            NonPrimaryCarTrips,
            when=lambda inst: inst.has_car_mode(),
            when_doc='if any "car_*" mode is defined',
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
        "linear_schedule": InputFile(LinearScheduleFile, optional=True),
        "tstars": InputFile(TstarsFile, optional=True),
        "car_driver_preferences": InputFile(
            CarDriverPreferencesFile,
            optional=True,
            when=lambda inst: inst.has_mode("car_driver"),
            when_doc='if the "car_driver" mode is defined',
        ),
        "car_driver_with_passengers_preferences": InputFile(
            CarDriverWithPassengersPreferencesFile,
            optional=True,
            when=lambda inst: inst.has_mode("car_driver_with_passengers"),
            when_doc='if the "car_driver_with_passengers" mode is defined',
        ),
        "car_passenger_preferences": InputFile(
            CarPassengerPreferencesFile,
            optional=True,
            when=lambda inst: inst.has_mode("car_passenger"),
            when_doc='if the "car_passenger" mode is defined',
        ),
        "car_ridesharing_preferences": InputFile(
            CarRidesharingPreferencesFile,
            optional=True,
            when=lambda inst: inst.has_mode("car_ridesharing"),
            when_doc='if the "car_ridesharing" mode is defined',
        ),
        "car_fuel": InputFile(
            CarFuelFile,
            optional=True,
            when=lambda inst: inst.has_car_mode(),
            when_doc='if any "car_*" mode is defined',
        ),
        "public_transit_preferences": InputFile(
            PublicTransitPreferencesFile,
            optional=True,
            when=lambda inst: inst.has_mode("public_transit"),
            when_doc='if the "public_transit" mode is defined',
        ),
        "walking_preferences": InputFile(
            WalkingPreferencesFile,
            optional=True,
            when=lambda inst: inst.has_mode("walking"),
            when_doc='if the "walking" mode is defined',
        ),
        "bicycle_preferences": InputFile(
            BicyclePreferencesFile,
            optional=True,
            when=lambda inst: inst.has_mode("bicycle"),
            when_doc='if the "bicycle" mode is defined',
        ),
    }
    output_files = {"metro_trips": MetroTripsFile}

    def is_defined(self) -> bool:
        if self.modes is None:
            return False
        # If there is no "trip mode", this step cannot be run (there is no trip to generate).
        return self.has_trip_mode()

    def run(self):
        import polars as pl

        trips: pl.DataFrame = self.input["trips"].read()
        if "destination_activity_duration" not in trips.columns:
            trips = trips.with_columns(
                destination_activity_duration=pl.lit(None, dtype=pl.Duration)
            )
        df = trips.select(
            "trip_id",
            "person_id",
            agent_id="tour_id",
            activity_time=pl_duration_to_seconds("destination_activity_duration").fill_null(0.0),
        ).sort("agent_id", "trip_id")
        # Set activity time to 0 for the last trip of the tour.
        df = df.with_columns(
            activity_time=pl.when(pl.col("trip_id") != pl.col("trip_id").last().over("agent_id"))
            .then("activity_time")
            .otherwise(0.0)
        )
        metro_trips = pl.DataFrame()
        for car_mode, vehicle_type in (
            ("car_driver", "car_driver_alone"),
            ("car_driver_with_passengers", "car_driver_multi"),
            ("car_passenger", "car_passenger"),
            ("car_ridesharing", "car_ridesharing"),
        ):
            if self.has_mode(car_mode):
                fuel_share = self.get_fuel_share(car_mode)
                car_trips = generate_car_trips(
                    car_mode,
                    vehicle_type,
                    df=df,
                    primary_trips_file=self.input["primary_car_trips"],
                    secondary_trips_file=self.input["secondary_car_trips"],
                    pref_file=self.input[f"{car_mode}_preferences"],
                    tstars_file=self.input["tstars"],
                    schedule_pref_file=self.input["linear_schedule"],
                    fuel_file=self.input["car_fuel"],
                    fuel_share=fuel_share,
                )
                metro_trips = pl.concat((metro_trips, car_trips), how="diagonal")
        if self.has_mode("public_transit"):
            public_transit_trips = generate_public_transit_trips(
                df,
                self.input["public_transit_travel_times"],
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
        metro_trips = metro_trips.drop("person_id")
        self.output["metro_trips"].write(metro_trips)

    def get_fuel_share(self, mode: str) -> float:
        """Returns the share of fuel cost that is paid by the individual, given the mode."""
        if mode == "car_driver":
            return 1.0
        elif mode == "car_ridesharing":
            return 1 / (1 + self.ridesharing_passenger_count)
        elif mode == "car_driver_with_passengers":
            # TODO. Make this configurable.
            return 1.0
        elif mode == "car_passenger":
            # TODO. Make this configurable.
            return 0.0
        else:
            return 0.0
