from datetime import time

import numpy as np
import polars as pl
from metro_common.errors import MetropyError, error_context
from metro_pipeline import Config, ConfigValue, Step

from .files import CLEAN_EDGES_FILE, EDGES_CAPACITIES_FILE

CAPACITIES = ConfigValue(
    "road_network.capacities",
    key="capacities",
    default=np.nan,
    expected_type=int | float | dict,
    description="Bottleneck capacity (in PCE/h) of edges.",
    note=(
        "The value is either a scalar value to be applied to all edges, a table "
        "`road_type -> capacity` or two tables `road_type -> capacity`, for urban and rural edges."
    ),
)


@error_context(msg="Cannot define bottleneck capacities")
def define_bottleneck_capacities(config: Config) -> bool:
    """Assign bottleneck capacities to each edge from exogenous values."""
    capacities = config[CAPACITIES]
    edges = CLEAN_EDGES_FILE.read(config)
    df = pl.from_pandas(edges.loc[:, ["edge_id", "road_type", "urban"]])
    df = df.with_columns(
        capacity=pl.lit(None, dtype=pl.Float64),
        times=pl.lit(None, dtype=pl.List(pl.Time)),
        capacities=pl.lit(None, dtype=pl.List(pl.Float64)),
    )
    if isinstance(capacities, float | int):
        # Case 1. Value is number.
        df = df.with_columns(capacity=pl.lit(capacities, dtype=pl.Float64))
    else:
        assert isinstance(capacities, dict)
        keys = set(capacities.keys())
        road_types = set(df["road_type"].unique())
        if keys == {"urban", "rural"}:
            # Case 3. Value is nested dict urban -> road_type -> capacity.
            df = df.with_columns(
                capacity=pl.when("urban")
                .then(
                    pl.col("road_type").replace_strict(capacities["urban"], return_dtype=pl.Float64)
                )
                .otherwise(
                    pl.col("road_type").replace_strict(capacities["rural"], return_dtype=pl.Float64)
                )
            )
        if keys == {"times", "values"}:
            # Case 4. Capacities are time-dependent, equal for all edges.
            assert all(isinstance(t, time) for t in capacities["times"]), (
                "Value `capacities.times` must be a list of time"
            )
            df = df.with_columns(
                times=pl.lit(capacities["times"]), capacities=pl.lit(capacities["values"])
            )
        elif all(k in road_types for k in keys):
            # Case 2. Value is dict road_type -> capacity.
            # Capacity value can be a constant or a dict with keys time / values.
            for road_type in keys:
                value = capacities[road_type]
                if isinstance(value, int | float):
                    df = df.with_columns(
                        capacity=pl.when(road_type=road_type)
                        .then(pl.lit(value))
                        .otherwise("capacity")
                    )
                elif isinstance(value, dict):
                    if "times" not in value.keys() and "values" not in value.keys():
                        raise MetropyError(
                            f"Expected `times` and `values` keys for capacities of road_type `{road_type}`"
                        )
                    if not isinstance(value["times"], list) and not all(
                        isinstance(t, time) for t in value["times"]
                    ):
                        raise MetropyError("Values for key `times` should be of Time type")
                    if not isinstance(value["values"], list) and not all(
                        isinstance(t, int | float) for t in value["times"]
                    ):
                        raise MetropyError("Values for key `values` should be numbers")
                    df = df.with_columns(
                        times=pl.when(road_type=road_type)
                        .then(pl.lit(value["times"]))
                        .otherwise("times"),
                        capacities=pl.when(road_type=road_type)
                        .then(pl.lit(value["values"]))
                        .otherwise("capacities"),
                    )
                else:
                    raise MetropyError(
                        f"Unexpected type for capacities values of road type `{road_type}`"
                    )
    df = df.drop("road_type", "urban")
    EDGES_CAPACITIES_FILE.save(df, config)
    return True


EXOGENOUS_CAPACITIES = Step(
    "exogenous-capacities",
    define_bottleneck_capacities,
    required_files=[CLEAN_EDGES_FILE],
    output_files=[EDGES_CAPACITIES_FILE],
    config_values=[CAPACITIES],
)
