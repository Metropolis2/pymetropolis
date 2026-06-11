from datetime import time
from typing import Any

from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_common.time import MetroTime
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import CustomParameter
from pymetropolis.metro_pipeline.steps import InputFile

from .files import RoadEdgesCapacitiesFile, RoadEdgesCleanFile, RoadEdgesUrbanFlagFile


def validate_capacity(value: Any) -> float | dict | None:
    """Returns the validated capacity if the given value is a valid input capacity (constant or
    time-dependent).

    The returned value is a float with the constant capacity or a dictionary with "times" (list of
    MetroTime) and "values" (list of float).
    """
    if isinstance(value, dict) and set(value.keys()) == {"times", "values"}:
        try:
            value["values"] = [float(v) for v in value["values"]]
        except ValueError:
            return None
        try:
            value["times"] = [MetroTime.parse(t) for t in value["times"]]
        except MetropyError:
            return None
        return value
    if isinstance(value, int | float):
        return value
    return None


def validate_capacity_map(value: dict) -> dict | None:
    """Returns True if the given value is a valid map str->capacity."""
    for k in value.keys():
        validated = validate_capacity(value[k])
        if validated is None:
            return None
        value[k] = validated
    return value


def capacities_validator(value: Any) -> int | float | dict:
    """Returns the value if it is a valid capacity input, otherwise raises an error."""
    validated = validate_capacity(value)
    if validated is not None:
        # Case 1. constant or time-depedent capacities
        return validated
    if isinstance(value, dict):
        keys = set(value.keys())
        if keys == {"urban", "rural"}:
            # Case 2. nested map urban/rural -> edge_type -> capacity
            for k in keys:
                validated = validate_capacity_map(value[k])
                if validated is None:
                    raise MetropyError(
                        f"Invalid {k} capacities (map edge_type->capacity expected): `{value[k]}`"
                    )
                value[k] = validated
            return value
        else:
            # Case 3. map edge_type -> capacity
            validated = validate_capacity_map(value)
            if validated is None:
                raise MetropyError(
                    f"Invalid capacities (map edge_type->capacity expected): `{value}`"
                )
            return validated
    else:
        raise MetropyError(f"Invalid capacities (number or dictionary expected): `{value}`")


class ExogenousCapacitiesStep(Step):
    """Generates bottleneck capacities for the road network edges, from exogenous values.

    The bottleneck capacities can be:

    - constant over edges
    - constant by edge type
    - constant by combinations of edge type and urban flag
    """

    capacities = CustomParameter(
        "road_network.capacities",
        validator=capacities_validator,
        validator_description=(
            "float (constant capacity for all edges), table with edge types as keys and capacities"
            ' as values, or table with "urban" and "rural" as keys and `edge_type->value` tables as'
            " values (see example)"
        ),
        description="Bottleneck capacity (in PCE/h) of edges.",
        example="""
```toml
[road_network.capacities]
[road_network.capacities.urban]
motorway = 2000
road = 1000
[road_network.capacities.rural]
motorway = 2000
road = 1500
```
        """,
    )
    input_files = {
        "clean_edges": RoadEdgesCleanFile,
        "urban_edges": InputFile(
            RoadEdgesUrbanFlagFile,
            when=lambda inst: inst.urban_flag_required(),
            when_doc="if default capacities rely on the urban flag",
        ),
    }
    output_files = {"edges_capacities": RoadEdgesCapacitiesFile}

    def is_defined(self) -> bool:
        return self.capacities is not None

    def urban_flag_required(self) -> bool:
        return isinstance(self.capacities, dict) and "urban" in self.capacities

    def run(self):
        import polars as pl

        capacities = self.capacities
        edges = self.input["clean_edges"].read()
        df = pl.from_pandas(edges.loc[:, edges.columns.isin(["edge_id", "edge_type"])])
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
            if "edge_type" not in df.columns:
                raise MetropyError("Edges have no `edge_type` column.")
            edge_types = set(df["edge_type"].unique())
            if keys == {"urban", "rural"}:
                urban_flags = self.input["urban_edges"].read()
                df = df.join(urban_flags, on="edge_id", how="left")
                # Case 3. Value is nested dict urban -> edge_type -> capacity.
                df = df.with_columns(
                    capacity=pl.when("urban")
                    .then(
                        pl.col("edge_type").replace_strict(
                            capacities["urban"], return_dtype=pl.Float64
                        )
                    )
                    .otherwise(
                        pl.col("edge_type").replace_strict(
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
            elif all(k in edge_types for k in keys):
                # Case 2. Value is dict edge_type -> capacity.
                # Capacity value can be a constant or a dict with keys time / values.
                for edge_type in keys:
                    value = capacities[edge_type]
                    if isinstance(value, int | float):
                        df = df.with_columns(
                            capacity=pl.when(edge_type=edge_type)
                            .then(pl.lit(value))
                            .otherwise("capacity")
                        )
                    elif isinstance(value, dict):
                        if "times" not in value.keys() and "values" not in value.keys():
                            raise MetropyError(
                                "Expected `times` and `values` keys for capacities of edge_type "
                                f"`{edge_type}`"
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
                            times=pl.when(edge_type=edge_type)
                            .then(pl.lit(value["times"]))
                            .otherwise("times"),
                            capacities=pl.when(edge_type=edge_type)
                            .then(pl.lit(value["values"]))
                            .otherwise("capacities"),
                        )
                    else:
                        raise MetropyError(
                            f"Unexpected type for capacities values of edge type `{edge_type}`"
                        )
        df = df.drop("edge_type", "urban", strict=False)
        self.output["edges_capacities"].write(df)
