import polars as pl
from metro_common.errors import MetropyError, error_context
from metro_common.types import FixedValues
from metro_demand.population import TRIPS_FILE, UNIFORM_DRAWS_FILE
from metro_pipeline import Config, ConfigTable, ConfigValue, Step

from .files import METRO_AGENTS_FILE

MODE_CHOICE_MODEL = ConfigValue(
    "mode_choice.model",
    key="model",
    expected_type=FixedValues(["Logit", "DrawnLogit", "DrawnNestedLogit", "Deterministic"]),
    default="Deterministic",
    description="Type of choice model for mode choice",
)

MODE_CHOICE_MU = ConfigValue(
    "mode_choice.mu",
    key="mu",
    expected_type=float,
    default=1.0,
    description="Value of mu for the Logit choice model",
    note="Only required when mode choice model is Logit",
)

MODE_CHOICE_TABLE = ConfigTable(
    "mode_choice",
    "mode_choice",
    items=[MODE_CHOICE_MODEL, MODE_CHOICE_MU],
)


@error_context(msg="Cannot write agents file")
def write_agents(config: Config):
    trips = TRIPS_FILE.read(config)
    agents = trips.select(agent_id="tour_id").unique().sort("agent_id")
    model = config[MODE_CHOICE_MODEL]
    if model == "Logit":
        agents = agents.with_columns(
            pl.lit("Logit").alias("alt_choice.type"),
            pl.lit(config[MODE_CHOICE_MU]).alias("alt_choice.mu"),
        )
    elif model == "DrawnLogit":
        # TODO: At this point the epsilons should be already drawn.
        raise MetropyError("TODO")
    elif model == "DrawnNestedLogit":
        raise MetropyError("TODO")
    elif model == "Deterministic":
        agents = agents.with_columns(
            pl.lit("Deterministic").alias("alt_choice.type"),
        )
    draws = UNIFORM_DRAWS_FILE.read(config)
    agents = agents.join(
        draws.select(pl.col("mode_u").alias("alt_choice.u"), agent_id="tour_id"),
        on="agent_id",
        how="left",
    )
    METRO_AGENTS_FILE.save(agents, config)
    return True


WRITE_AGENTS_STEP = Step(
    "write-agents",
    write_agents,
    required_files=[TRIPS_FILE, UNIFORM_DRAWS_FILE],
    output_files=[METRO_AGENTS_FILE],
    config_values=[MODE_CHOICE_MODEL, MODE_CHOICE_MU],
)
