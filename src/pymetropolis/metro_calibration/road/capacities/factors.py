from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_calibration.road.free_flow.variables import (
    DEFAULT_DEFINITIONS,
    definition_to_expression,
    interval_labels,
    variable_definition_validator,
)
from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_network.road_network.files import RoadEdgesCleanFile, RoadEdgesUrbanFlagFile
from pymetropolis.metro_pipeline.parameters import CustomParameter, ListParameter
from pymetropolis.metro_pipeline.steps import InputFile, Step
from pymetropolis.metro_pipeline.types import String

from .files import EdgeCapacityFactorsFile

if TYPE_CHECKING:
    import polars as pl


def variable_modalities(variable: str, definition: dict) -> tuple[pl.Expr, str | None]:
    """Returns an expression giving the modality (as a String) of the variable for each edge, and
    the first modality of the variable (`None` if it cannot be known in advance).

    The first modality is `"false"` for booleans, the lowest interval for intervals and the first
    value of the `map` for categories (if the map is defined).
    """
    import polars as pl

    expr = definition_to_expression(variable, definition)
    match definition["type"]:
        case "boolean":
            return pl.when(expr).then(pl.lit("true")).otherwise(pl.lit("false")), "false"
        case "intervals":
            cuts = sorted(map(float, definition["cuts"]))
            right_closed = not definition.get("left_closed", False)
            return expr.cast(pl.String), interval_labels(cuts, right_closed)[0]
        case "categories":
            first = next(iter(definition["map"].values())) if "map" in definition else None
            return expr.cast(pl.String), first
        case _:
            raise MetropyError(
                f"Variable `{variable}` cannot be used as a capacity factor: type "
                f"`{definition['type']}` is not supported (use boolean, intervals or categories)"
            )


class WriteEdgeCapacityFactorsStep(Step):
    """Generates the input edge-factors file for Metropolis-Core, used to calibrate the bottleneck
    capacities.

    The variables listed in `calibration.capacity.variables` are defined according to the rules of
    `road_network.capacities_calibration.variables` (or the default definitions of
    `FreeFlowRoadEdgesVariablesStep` for variables without rule, e.g., `traffic_signals` as a
    boolean). Continuous variables are not supported.

    Each edge gets one row for each variable modality that applies to it. All the modalities of the
    first variable are considered (so that each edge gets at least one row), while the first
    modality of the other variables is excluded (it acts as the reference modality). The first
    modality is `false` for booleans, the lowest interval for intervals, and the first value of
    `map` for categories (the alphabetically first observed value when `map` is not defined).
    Edges with a null value for a variable get no row for that variable.
    """

    capacity_variable_definitions = CustomParameter(
        "road_network.capacities_calibration.variables",
        validator=variable_definition_validator,
        description="Definition of the variables (type and modalities) for capacity calibration.",
        validator_description=(
            "Table variable->definition, with the same format as "
            "`road_network.free_flow_calibration.variables`."
        ),
    )
    variables = ListParameter(
        "calibration.capacity.variables",
        inner=String(),
        description="Name of the variables used as capacity factors.",
        note="All modalities of the first variable are used, so that each edge gets a factor.",
        example='`["edge_type", "lanes", "traffic_signals"]`',
        min_length=1,
    )

    input_files = {
        "clean_edges": RoadEdgesCleanFile,
        "urban": InputFile(
            RoadEdgesUrbanFlagFile,
            when=lambda inst: "urban" in inst.variables,
            when_doc='if "urban" is in `calibration.capacity.variables`',
        ),
    }
    output_files = {"edge_factors": EdgeCapacityFactorsFile}

    def is_defined(self) -> bool:
        return self.variables is not None

    def run(self):
        import polars as pl

        assert self.variables

        definitions = DEFAULT_DEFINITIONS | (self.capacity_variable_definitions or {})

        edges = pl.from_pandas(
            self.input["clean_edges"].read().drop(columns="geometry", errors="ignore")
        )
        if "urban" in self.variables:
            urban_flags = self.input["urban"].read()
            edges = edges.join(urban_flags, on="edge_id", how="left")

        factors = list()
        for i, variable in enumerate(self.variables):
            if variable not in definitions:
                raise MetropyError(f"No definition for capacity variable `{variable}`")
            if variable not in edges.columns:
                raise MetropyError(
                    f"Cannot define variable for `{variable}`: column does not exist in edges"
                )
            modality, first = variable_modalities(variable, definitions[variable])
            df = edges.select("edge_id", modality=modality).filter(pl.col("modality").is_not_null())
            if i == 0:
                n = edges["edge_id"].n_unique() - df["edge_id"].n_unique()
                if n > 0:
                    logger.warning(
                        f"{n} edges have a null value for the first capacity variable `{variable}`"
                    )
            else:
                if first is None:
                    first = df["modality"].min()
                df = df.filter(pl.col("modality") != first)
            factors.append(
                df.select("edge_id", factor=pl.concat_str(pl.lit(f"{variable}="), "modality"))
            )
        df = pl.concat(factors).sort("edge_id", "factor")

        logger.debug(f"Factor counts:\n{df['factor'].value_counts(sort=True)}")
        self.output["edge_factors"].write(df)
