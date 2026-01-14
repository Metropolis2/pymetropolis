import numpy as np
import polars as pl
from metro_common.errors import MetropyError, error_context
from metro_pipeline import Config, ConfigValue, Step
from typeguard import TypeCheckError, check_type

from .files import CLEAN_EDGES_FILE, EDGES_PENALTIES_FILE

PENALTIES = ConfigValue(
    "road_network.penalties",
    key="penalties",
    expected_type=float | dict[str, float] | dict[str, dict[str, float]],
    description="Constant time penalty (in seconds) of edges.",
    note=(
        "The value is either a scalar value to be applied to all edges, a table "
        "`road_type -> penalty` or two tables `road_type -> penalty`, for urban and rural edges."
    ),
)


@error_context(msg="Cannot define edge penalties")
def define_edge_penalties(config: Config) -> bool:
    """Assign constant time penalties to each edge from exogenous values."""
    penalties = config[PENALTIES]
    edges = CLEAN_EDGES_FILE.read(config)
    if isinstance(penalties, float | int):
        # Case 1. Value is number.
        edges["constant"] = penalties
    else:
        # Case 2. Value is dict road_type -> penalty.
        try:
            check_type(penalties, dict[str, float])
            edges["constant"] = edges["road_type"].map(penalties)
        except TypeCheckError:
            # Case 3. Value is nested dict urban -> road_type -> penalty.
            try:
                check_type(penalties, dict[str, dict[str, float]])
            except TypeCheckError:
                pass
            else:
                if "urban" not in penalties.keys() or "rural" not in penalties.keys():
                    raise MetropyError("Missing keys `urban` or `rural`")
                edges["constant"] = np.nan
                mask = edges["urban"]
                edges.loc[mask, "constant"] = edges.loc[mask, "road_type"].map(penalties["urban"])
                mask = ~edges["urban"]
                edges.loc[mask, "constant"] = edges.loc[mask, "road_type"].map(penalties["rural"])
    df = pl.from_pandas(edges[["edge_id", "constant"]])
    df = df.with_columns(pl.col("constant").cast(pl.Float64))
    EDGES_PENALTIES_FILE.save(df, config)
    return True


EXOGENOUS_CAPACITIES = Step(
    "exogenous-edge-penalties",
    define_edge_penalties,
    required_files=[CLEAN_EDGES_FILE],
    output_files=[EDGES_PENALTIES_FILE],
    config_values=[PENALTIES],
)
