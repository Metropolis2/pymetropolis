import polars as pl

from pymetropolis.metro_common.errors import error_context
from pymetropolis.metro_demand.population import TRIPS_FILE
from pymetropolis.metro_pipeline import Config, ConfigTable, ConfigValue, Step

from .files import OUTSIDE_OPTION_PARAMETERS_FILE

CONSTANT = ConfigValue(
    "modes.outside_option.constant",
    key="constant",
    default=0.0,
    expected_type=float,
    description="Constant utility of the outside option (€).",
)

OUTSIDE_OPTION_TABLE = ConfigTable("modes.outside_option", "outside_option", items=[CONSTANT])


@error_context(msg="Cannot generate outside option preferences.")
def generate_outside_option_parameters(config: Config):
    trips = TRIPS_FILE.read(config)
    df = (
        trips.select("tour_id", outside_option_cst=pl.lit(config[CONSTANT]))
        .unique()
        .sort("tour_id")
    )
    OUTSIDE_OPTION_PARAMETERS_FILE.save(df, config)
    return True


OUTSIDE_OPTION_PREFERENCES_STEP = Step(
    "outside-option-preferences",
    generate_outside_option_parameters,
    required_files=[TRIPS_FILE],
    output_files=[OUTSIDE_OPTION_PARAMETERS_FILE],
    config_values=[CONSTANT],
)
