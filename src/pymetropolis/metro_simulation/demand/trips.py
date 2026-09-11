from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_common.errors import error_context
from pymetropolis.metro_common.utils import pl_duration_to_seconds
from pymetropolis.metro_demand.departure_time import LinearScheduleFile, TstarsFile
from pymetropolis.metro_demand.modes import MODE_PREFERENCES_FILES, PublicTransitPreferencesFile
from pymetropolis.metro_demand.modes.files import (
    BicyclePreferencesFile,
    BicycleTravelTimesFile,
    WalkingPreferencesFile,
    WalkingTravelTimesFile,
)
from pymetropolis.metro_demand.population import TripsFile
from pymetropolis.metro_demand.population.files import HouseholdsFile, PersonsFile, ToursModeFile
from pymetropolis.metro_demand.routing.files import (
    NonPrimaryCarTrips,
    PrimaryCarTripsAccessEgressFile,
    TripsPublicTransitItinerariesFile,
)
from pymetropolis.metro_environment.fuel.files import CarFuelFile
from pymetropolis.metro_pipeline import PopulationStep, Step
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_simulation.common import StepWithRidesharingCount, merge_populations
from pymetropolis.modes import StepWithModes

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
    trips = trips.with_columns(can_drive=pl.col("has_car") & pl.col("has_driving_license"))
    if "destination_activity_duration" not in trips.columns:
        trips = trips.with_columns(destination_activity_duration=pl.lit(None, dtype=pl.Duration))
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
    return trips.select("trip_id", "agent_id", "activity_time", "has_car", "can_drive")


@error_context(msg="Cannot generate car trips")
def generate_car_trips(
    mode: str,
    vehicle_type: str,
    df: pl.DataFrame,
    primary_trips_file: PrimaryCarTripsAccessEgressFile,
    secondary_trips_file: NonPrimaryCarTrips,
    pref_file: MetroDataFrameFile | None = None,
    tstars_file: TstarsFile | None = None,
    schedule_pref_file: LinearScheduleFile | None = None,
    fuel_file: CarFuelFile | None = None,
    fuel_share: float | None = None,
):
    import polars as pl

    # Car modes are only accessible to car owners.
    # Note. This an assumption. In real life, you can be a car driver or passenger without owning a
    # car (e.g., car rental, taxi, ridesharing with someone from another household).
    df = df.filter("has_car")
    # Car-driver modes are only accessible to driving license holders.
    # Note. This assumption does not apply to the "car_ridesharing" mode.
    if "driver" in mode:
        df = df.filter("can_drive")
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
    if pref_file is not None and pref_file.exists():
        # The mode constant is defined at the tour level so it is only added at the alternative
        # level.
        params: pl.DataFrame = pref_file.read().select(
            agent_id="tour_id", alpha=pl.col(f"{mode}_vot") / 3600.0
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
    df = df.drop("activity_time", "access_time_sec", "egress_time_sec")
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


class PrepareMetroTripsStep(StepWithModes, StepWithRidesharingCount, PopulationStep):
    """Prepares the trips for the Metropolis-Core simulation."""

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
        "car_fuel": InputFile(
            CarFuelFile,
            optional=True,
            when=lambda inst: inst.has_car_mode(),
            when_doc=r'if any "car\_\*" mode is defined',
        ),
        **{
            f"{mode}_preferences": InputFile(
                pref_file,
                optional=True,
                when=lambda inst, mode=mode: inst.has_mode(mode),
                when_doc=f'if the "{mode}" mode is defined',
            )
            for mode, pref_file in MODE_PREFERENCES_FILES.items()
        },
    }
    output_files = {"metro_trips": MetroTripsPopulationFile}

    def is_defined(self) -> bool:
        if self.modes is None:
            return False
        # If there is no "trip mode", this step cannot be run (there is no trip to generate).
        return self.has_trip_mode()

    def run(self):
        import polars as pl

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
        df = clean_trips(trips)
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
        metro_trips = metro_trips.drop("has_car", "can_drive").sort("agent_id", "alt_id", "trip_id")
        self.output["metro_trips"].write(metro_trips)

    def get_fuel_share(self, mode: str) -> float:
        """Returns the share of fuel cost that is paid by the individual, given the mode."""
        if mode == "car_driver":
            return 1.0
        elif mode == "car_ridesharing":
            assert self.ridesharing_passenger_count is not None
            return 1 / (1 + self.ridesharing_passenger_count)
        elif mode == "car_driver_with_passengers":
            # TODO. Make this configurable.
            return 1.0
        elif mode == "car_passenger":
            # TODO. Make this configurable.
            return 0.0
        else:
            return 0.0


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
        for car_mode, vehicle_type in (
            ("car_driver", "car_driver_alone"),
            ("car_driver_with_passengers", "car_driver_multi"),
            ("car_passenger", "car_passenger"),
            ("car_ridesharing", "car_ridesharing"),
        ):
            if self.has_mode(car_mode):
                car_trips = generate_car_trips(
                    car_mode,
                    vehicle_type,
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
