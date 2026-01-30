import polars as pl

from pymetropolis.metro_common.errors import error_context
from pymetropolis.metro_common.utils import time_to_seconds_since_midnight_pl
from pymetropolis.metro_demand.departure_time import LinearScheduleFile, TstarsFile
from pymetropolis.metro_demand.modes import (
    CarDriverODsFile,
    CarDriverPreferencesFile,
    PublicTransitPreferencesFile,
    PublicTransitTravelTimesFile,
)
from pymetropolis.metro_demand.population import TripsFile
from pymetropolis.metro_pipeline.steps import InputFile

from .common import StepWithModes
from .files import MetroTripsFile


@error_context(msg="Cannot generate car driver trips")
def generate_car_driver_trips(
    df: pl.DataFrame,
    ods_file: CarDriverODsFile,
    pref_file: CarDriverPreferencesFile,
    tstars_file: TstarsFile,
    schedule_pref_file: LinearScheduleFile,
):
    df = df.with_columns(
        pl.lit("car_driver").alias("alt_id"),
        pl.lit("Road").alias("class.type"),
        pl.lit("car_driver").alias("class.vehicle"),
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
        params: pl.DataFrame = pref_file.read()
        df = (
            df.join(params, on="person_id", how="left")
            .with_columns(
                constant_utility=-pl.col("car_driver_cst"),
                alpha=pl.col("car_driver_vot") / 3600.0,
            )
            .drop("car_driver_cst", "car_driver_vot")
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


class WriteMetroTripsStep(StepWithModes):
    """Generates the input trips file for the Metropolis-Core simulation."""

    input_files = {
        "trips": TripsFile,
        "car_driver_ods": InputFile(
            CarDriverODsFile,
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
        return self.has_trip_modes()

    def run(self):
        trips: pl.DataFrame = self.input["trips"].read()
        df = trips.select("trip_id", "person_id", agent_id="tour_id").sort("agent_id", "trip_id")
        metro_trips = pl.DataFrame()
        if self.has_mode("car_driver"):
            car_driver_trips = generate_car_driver_trips(
                df,
                self.input["car_driver_ods"],
                self.input["car_driver_preferences"],
                self.input["tstars"],
                self.input["linear_schedule"],
            )
            metro_trips = pl.concat((metro_trips, car_driver_trips), how="diagonal")
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
