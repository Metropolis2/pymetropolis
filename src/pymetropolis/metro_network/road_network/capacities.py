from datetime import time
from typing import Any

import numpy as np
import polars as pl

from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import CustomParameter

from .files import CleanEdgesFile, EdgesCapacitiesFile


def is_valid_capacity(value: Any) -> bool:
    """Returns True if the given value is a valid capacity (constant or time-dependent)."""
    if isinstance(value, int | float):
        return True
    if isinstance(value, dict) and set(value.keys()) == {"times", "values"}:
        return all(isinstance(t, time) for t in value["times"]) and all(
            isinstance(c, int | float) for c in value["values"]
        )
    return False


def is_valid_capacity_map(value: dict) -> bool:
    """Returns True if the given value is a valid map str->capacity."""
    for k in value.keys():
        if not is_valid_capacity(value[k]):
            return False
    return True


def capacities_validator(value: Any) -> int | float | dict:
    """Returns the value if it is a valid capacity input, otherwise raises an error."""
    if is_valid_capacity(value):
        # Case 1. constant capacity
        return value
    elif isinstance(value, dict):
        keys = set(value.keys())
        if keys == {"urban", "rural"}:
            # Case 2. nested map urban/rural -> road_type -> capacity
            for k in keys:
                if not is_valid_capacity_map(value[k]):
                    raise MetropyError(
                        f"Invalid {k} capacities (map road_type->capacity expected): `{value[k]}`"
                    )
            else:
                return value
        else:
            # Case 3. map road_type -> capacity
            if is_valid_capacity_map(value):
                return value
            else:
                raise MetropyError(
                    f"Invalid capacities (map road_type->capacity expected): `{value}`"
                )
    else:
        raise MetropyError(f"Invalid capacities (number or dictionary expected): `{value}`")


class ExogenousCapacitiesStep(Step):
    # TODO: Add custom validation for the parameter.
    capacities = CustomParameter(
        "road_network.capacities",
        validator=capacities_validator,
        default=np.nan,
        description="Bottleneck capacity (in PCE/h) of edges.",
        note=(
            "The value is either a scalar value to be applied to all edges, a table "
            "`road_type -> capacity` or two tables `road_type -> capacity`, for urban and rural edges."
        ),
    )
    output_files = {"edges_capacities": EdgesCapacitiesFile}

    def required_files(self):
        return {"clean_edges": CleanEdgesFile}

    def run(self):
        capacities = self.capacities
        edges = self.input["clean_edges"].read()
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
                        pl.col("road_type").replace_strict(
                            capacities["urban"], return_dtype=pl.Float64
                        )
                    )
                    .otherwise(
                        pl.col("road_type").replace_strict(
                            capacities["rural"], return_dtype=pl.Float64
                        )
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
        self.output["edges_capacities"].write(df)
