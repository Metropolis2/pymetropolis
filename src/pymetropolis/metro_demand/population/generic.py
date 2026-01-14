import polars as pl

from pymetropolis.metro_common.errors import error_context
from pymetropolis.metro_demand.modes.files import CAR_DRIVER_ODS_FILE
from pymetropolis.metro_pipeline import Config, Step

from .files import HOUSEHOLDS_FILE, PERSONS_FILE, TRIPS_FILE


@error_context(msg="Cannot generate generic population from trips.")
def generate_generic_population(config: Config):
    df = CAR_DRIVER_ODS_FILE.read(config)
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
        age=pl.lit(None, dtype=pl.UInt8),
        employed=pl.lit(None, dtype=pl.Boolean),
        woman=pl.lit(None, dtype=pl.Boolean),
        socioprofessional_class=pl.lit(None, dtype=pl.UInt8),
        has_driving_license=pl.lit(True, dtype=pl.Boolean),
        has_pt_subscription=pl.lit(True, dtype=pl.Boolean),
    )
    households = trips.select(
        "household_id",
        number_of_persons=pl.lit(1, dtype=pl.UInt8),
        number_of_vehicles=pl.lit(1, dtype=pl.UInt8),
        number_of_bikes=pl.lit(1, dtype=pl.UInt8),
        income=pl.lit(None, dtype=pl.Float64),
    )
    TRIPS_FILE.save(trips, config)
    PERSONS_FILE.save(persons, config)
    HOUSEHOLDS_FILE.save(households, config)
    return True


GENERIC_POPULATION_STEP = Step(
    "generic-population",
    generate_generic_population,
    required_files=[CAR_DRIVER_ODS_FILE],
    output_files=[TRIPS_FILE, PERSONS_FILE, HOUSEHOLDS_FILE],
)
