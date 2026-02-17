import polars as pl

from pymetropolis.metro_common.errors import error_context
from pymetropolis.metro_common.utils import time_to_seconds_since_midnight_pl
from pymetropolis.metro_demand.departure_time import LinearScheduleFile, TstarsFile
from pymetropolis.metro_demand.modes import (
    PublicTransitPreferencesFile,
    PublicTransitTravelTimesFile,
)
from pymetropolis.metro_demand.modes.car import (
    CarDriverPreferencesFile,
    CarDriverWithPassengersPreferencesFile,
    CarFuelFile,
    CarODsFile,
    CarPassengerPreferencesFile,
    CarRidesharingPreferencesFile,
)
from pymetropolis.metro_demand.population import TripsFile
from pymetropolis.metro_pipeline.file import MetroDataFrameFile
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_simulation.common import StepWithModes, StepWithRidesharingCount

from .files import MetroTripsFile


@error_context(msg="Cannot generate car trips")
def generate_car_trips(
    mode: str,
    vehicle_type: str,
    df: pl.DataFrame,
    ods_file: CarODsFile,
    pref_file: MetroDataFrameFile,
    tstars_file: TstarsFile,
    schedule_pref_file: LinearScheduleFile,
    fuel_file: CarFuelFile,
    fuel_share: float,
):
    df = df.with_columns(
        pl.lit(mode).alias("alt_id"),
        pl.lit("Road").alias("class.type"),
        pl.lit(vehicle_type).alias("class.vehicle"),
    )
    ods: pl.DataFrame = ods_file.read()
    df = (
        df.join(ods, on="trip_id", how="left")
        .with_columns(
            pl.col("origin_node_id").alias("class.origin"),
            pl.col("destination_node_id").alias("class.destination"),
        )
        .drop("origin_node_id", "destination_node_id")
    )
    if pref_file.exists():
        params: pl.DataFrame = pref_file.read().select(
            "person_id",
            constant_utility=-pl.col(f"{mode}_cst"),
            alpha=pl.col(f"{mode}_vot") / 3600.0,
        )
        df = df.join(params, on="person_id", how="left")
    if tstars_file.exists():
        tstars: pl.DataFrame = tstars_file.read()
        df = (
            df.join(tstars, on="trip_id", how="left")
            .with_columns(
                time_to_seconds_since_midnight_pl(pl.col("tstar")).alias("schedule_utility.tstar")
            )
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
                pl.col("delta").dt.total_seconds().cast(pl.Float64).alias("schedule_utility.delta"),
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
    return df


@error_context(msg="Cannot generate public-transit trips")
def generate_public_transit_trips(
    df: pl.DataFrame,
    tts_file: PublicTransitTravelTimesFile,
    pref_file: PublicTransitPreferencesFile,
    tstars_file: TstarsFile,
    schedule_pref_file: LinearScheduleFile,
):
    df = df.with_columns(
        pl.lit("public_transit").alias("alt_id"),
        pl.lit("Virtual").alias("class.type"),
    )
    tts: pl.DataFrame = tts_file.read()
    df = (
        df.join(tts, on="trip_id", how="left")
        .with_columns(
            pl.col("public_transit_travel_time")
            .dt.total_seconds()
            .cast(pl.Float64)
            .alias("class.travel_time")
        )
        .drop("public_transit_travel_time")
    )
    if pref_file.exists():
        params: pl.DataFrame = pref_file.read()
        df = (
            df.join(params, on="person_id", how="left")
            .with_columns(
                constant_utility=-pl.col("public_transit_cst"),
                alpha=pl.col("public_transit_vot") / 3600.0,
            )
            .drop("public_transit_cst", "public_transit_vot")
        )
    if tstars_file.exists():
        tstars: pl.DataFrame = tstars_file.read()
        df = (
            df.join(tstars, on="trip_id", how="left")
            .with_columns(
                time_to_seconds_since_midnight_pl(pl.col("tstar")).alias("schedule_utility.tstar")
            )
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
                pl.col("delta").dt.total_seconds().cast(pl.Float64).alias("schedule_utility.delta"),
            )
            .drop("beta", "gamma", "delta")
        )
    return df


class WriteMetroTripsStep(StepWithModes, StepWithRidesharingCount):
    """Generates the input trips file for the Metropolis-Core simulation."""

    input_files = {
        "trips": TripsFile,
        "car_driver_ods": InputFile(
            CarODsFile,
            when=lambda inst: inst.has_mode("car_driver"),
            when_doc='if the "car_driver" mode is defined',
        ),
        "public_transit_travel_times": InputFile(
            PublicTransitTravelTimesFile,
            when=lambda inst: inst.has_mode("public_transit"),
            when_doc='if the "public_transit" mode is defined',
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
            when_doc='if the "car_driver" mode is defined',
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
    }
    output_files = {"metro_trips": MetroTripsFile}

    def is_defined(self) -> bool:
        if self.modes is None:
            return False
        # If there is no "trip mode", this step cannot be run (there is no trip to generate).
        return self.has_trip_mode()

    def run(self):
        trips: pl.DataFrame = self.input["trips"].read()
        df = trips.select("trip_id", "person_id", agent_id="tour_id").sort("agent_id", "trip_id")
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
                    ods_file=self.input["car_driver_ods"],
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
