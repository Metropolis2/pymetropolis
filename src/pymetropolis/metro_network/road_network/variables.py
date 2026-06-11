from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_network.road_network.files import (
    RoadEdgesCleanFile,
    RoadEdgesUrbanFlagFile,
    RoadEdgesVariablesFile,
)
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import CustomParameter
from pymetropolis.metro_pipeline.steps import InputFile

if TYPE_CHECKING:
    import polars as pl

DEFAULT_DEFINITIONS = {
    "toll": {"type": "boolean"},
    "roundabout": {"type": "boolean"},
    "give_way": {"type": "boolean"},
    "stop": {"type": "boolean"},
    "traffic_signals": {"type": "boolean"},
    "edge_type": {"type": "categories"},
    "speed_limit": {"type": "continuous"},
    "lanes": {"type": "continuous"},
    "target_in_degree": {"type": "continuous"},
    "source_in_degree": {"type": "continuous"},
    "target_out_degree": {"type": "continuous"},
    "source_out_degree": {"type": "continuous"},
    "urban": {"type": "boolean"},
}


def variable_definition_validator(value: Any) -> dict[str, Any]:
    """A validator function for a parameter that defines a variable.

    Returns the value if it is a valid parameter input, otherwise raises an error.

    The value must be a dictionary with key "type".

    There are four possible types:
        - "boolean"
        - "intervals" with key "cuts" (list of numbers) and "left_closed" (optional, boolean)
        - "continuous" with key "min" (optional, number) and "max" (optional, number)
        - "categories" with key "map" (optional, dictionary)
    """
    if not isinstance(value, dict):
        raise MetropyError(f"Invalid variable definitions: table expected, got `{value}`")
    for key, item in value.items():
        if not isinstance(item, dict):
            raise MetropyError(
                f"Invalid definition for variable `{key}`: table expected, got `{item}`"
            )
        validate_variable_definition_inner(key, item)
    return value


def validate_variable_definition_inner(name: str, definition: dict):
    if "type" not in definition:
        raise MetropyError(f"Invalid definition for variable `{name}`: type is not defined")
    match definition["type"]:
        case "boolean":
            pass
        case "intervals":
            if "cuts" not in definition:
                raise MetropyError(
                    f"Invalid definition for variable `{name}`: cuts are not defined"
                )
            if not isinstance(definition["cuts"], list) and not all(
                isinstance(c, int | float) for c in definition["cuts"]
            ):
                raise MetropyError(
                    f"Invalid definition for variable `{name}`: list of numbers expected for "
                    f"`cuts`, got `{definition['cuts']}`"
                )
            if "left_closed" in definition and not isinstance(definition["left_closed"], bool):
                raise MetropyError(
                    f"Invalid definition for variable `{name}`: boolean expected for "
                    f"`left_closed`, got `{definition['left_closed']}`"
                )
        case "continuous":
            if "min" in definition and not isinstance(definition["min"], int | float):
                raise MetropyError(
                    f"Invalid definition for variable `{name}`: number expected for `min`, "
                    f"got `{definition['min']}`"
                )
            if "max" in definition and not isinstance(definition["max"], int | float):
                raise MetropyError(
                    f"Invalid definition for variable `{name}`: number expected for `max`, "
                    f"got `{definition['max']}`"
                )
        case "categories":
            if "map" in definition and not isinstance(definition["map"], dict):
                raise MetropyError(
                    f"Invalid definition for variable `{name}`: dictionary expected for `map`, "
                    f"got `{definition['map']}`"
                )


def definition_to_expression(col: str, definition: dict) -> pl.Expr:
    """Converts the definition of a variable in a polars Expression."""
    import polars as pl

    match definition["type"]:
        case "boolean":
            return pl.col(col).cast(pl.Boolean)
        case "intervals":
            return (
                pl.col(col)
                .cut(
                    definition["cuts"],
                    left_closed=definition.get("left_closed", False),
                    include_breaks=True,
                )
                .struct.field("category")
                .cast(pl.String)
                .str.replace(r"\s", "")
                .cast(pl.Categorical)
                .alias(col)
            )
        case "categories":
            expr = pl.col(col)
            if "map" in definition:
                expr = expr.replace_strict(definition["map"], default=None)
            return expr.cast(pl.Categorical)
        case "continuous":
            expr = pl.col(col).cast(pl.Float64)
            if "min" in definition:
                expr = expr.clip(lower_bound=definition["min"])
            if "max" in definition:
                expr = expr.clip(upper_bound=definition["max"])
            return expr
        case _:
            raise MetropyError(f"Variable type `{definition['type']}` is not supported.")


class RoadEdgesVariablesStep(Step):
    """This Step generates edge-level variables to be used in the calibration process."""

    variable_definitions = CustomParameter(
        "road_network.calibration_variables",
        validator=variable_definition_validator,
        description=(
            "Definition of the variables (type and modalities) available for road network "
            "calibration.",
        ),
        validator_description=(
            "Table variable->definition, where `variable` is the name of a valid edge column and "
            "`definition` is a table with key `type`. "
            'Four types are possible: `"boolean"`, `"continuous"`, `"intervals"`, `"categories"`. '
            "See the example for configuration options."
        ),
        example="""
```toml
[road_network.calibration_variables]
[road_network.calibration_variables.urban]
type = "boolean"

[road_network.calibration_variables.lanes]
type = "continuous"
min = 1  # Optional lower bound to constrain the values.
max = 3  # Optional upper bound to constrain the values.

[road_network.calibration_variables.speed_limit]
type = "intervals"
cuts = [30, 50, 70, 90, 110, 130]  # Breakpoints of the intervals.
left_closed = true  # Set the intervals to be left-closed instead of right-closed.

[road_network.calibration_variables.edge_type]
type = "categories"
# Optionally, group categories together with `map` (all original modalities must be defined).
map = {"motorway": "motorway", "motorway_link": "motorway", ...}
```
        """,
    )
    input_files = {
        "edges": RoadEdgesCleanFile,
        "urban": InputFile(RoadEdgesUrbanFlagFile, optional=True),
    }
    output_files = {"variables": RoadEdgesVariablesFile}
    priority = 0

    def run(self):
        import polars as pl

        # Overwrite default definitions with custom definitions.
        definitions = DEFAULT_DEFINITIONS
        if self.variable_definitions is not None:
            definitions.update(self.variable_definitions)

        edges = self.input["edges"].read()
        df = pl.from_pandas(
            edges.drop(
                columns=["source", "target", "name", "original_id", "geometry"], errors="ignore"
            )
        )
        if self.input["urban"].exists():
            urban_flags = self.input["urban"].read()
            df = df.join(urban_flags, on="edge_id")

        variables = []
        for variable, definition in definitions.items():
            if variable in df.columns:
                variables.append(definition_to_expression(variable, definition))
            elif variable in self.variable_definitions:
                # Variable is explicitly defined but is not available in the data.
                raise MetropyError(
                    f"Cannot define variable for `{variable}`: column does not exist in edges"
                )

        df = df.select("edge_id", *variables)
        self.output["variables"].write(df)
