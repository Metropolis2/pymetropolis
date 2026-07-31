from __future__ import annotations

from math import inf
from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_common.io import read_dataframe
from pymetropolis.metro_network.road_network.common import default_edge_values_validator
from pymetropolis.metro_network.road_network.files import RoadEdgesCleanFile, RoadEdgesUrbanFlagFile
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import CustomParameter, FloatParameter, PathParameter
from pymetropolis.metro_pipeline.steps import InputFile

from .files import (
    RoadEdgesFreeFlowTravelTimeFile,
    RoadEdgesPenaltiesFile,
    RoadEdgesPenaltyCoefficientsFile,
    RoadEdgesVariablesFile,
)

if TYPE_CHECKING:
    import geopandas as gpd
    import polars as pl


EPSILON = 1e-8


def check_bounds(df: pl.DataFrame, col: str, lb: float | None, ub: float | None) -> pl.DataFrame:
    import polars as pl

    # Check bounds.
    if lb is not None:
        n = (df[col] < lb).sum()
        if n:
            share = n / len(df)
            logger.warning(
                f"Value {col} is smaller than {lb} for {n} edges ({share:.2%}). "
                f"Values are forced to {lb}."
            )
    if ub is not None:
        n = (df[col] > ub).sum()
        if n:
            share = n / len(df)
            logger.warning(
                f"Value {col} is larger than {ub} for {n} edges ({share:.2%}). "
                f"Values are set to {ub}."
            )
    df = df.with_columns(pl.col(col).clip(lower_bound=lb, upper_bound=ub))
    return df


class ExogenousEdgePenaltiesStep(Step):
    """Generates travel time penalties for the road network edges, from exogenous values.

    The penalties can be:

    - constant over edges
    - constant by edge type
    - constant by combinations of edge type and urban flag
    """

    penalties = CustomParameter(
        "road_network.penalties",
        validator=default_edge_values_validator,
        description="Constant time penalty (in seconds) of edges.",
        validator_description=(
            "float (constant penalty for all edges), table with edge types as keys and penalties"
            ' as values, or table with "urban" and "rural" as keys and `edge_type->value` tables as'
            " values (see example)"
        ),
        example="""
```toml
[road_network.penalties]
[road_network.penalties.urban]
motorway = 0
road = 5
[road_network.penalties.rural]
motorway = 0
road = 2
```
        """,
    )
    speed_multiplier = CustomParameter(
        "road_network.speed_multiplier",
        validator=default_edge_values_validator,
        description="By how much edge speed limit must be multiplied to get edge free-flow speed.",
        validator_description=(
            "float (constant multiplier for all edges), table with edge types as keys and "
            'multiplier as values, or table with "urban" and "rural" as keys and '
            "edge_type->value` tables as values (see example)"
        ),
        example="""
```toml
[road_network.speed_multiplier]
[road_network.speed_multiplier.urban]
motorway = 0.9
road = 0.8
[road_network.speed_multiplier.rural]
motorway = 1.0
road = 0.9
```
        """,
    )
    input_files = {
        "clean_edges": RoadEdgesCleanFile,
        "urban_edges": InputFile(
            RoadEdgesUrbanFlagFile,
            when=lambda inst: inst.urban_flag_required(),
            when_doc="if default penalties rely on the urban flag",
        ),
    }
    output_files = {"edges_penalties": RoadEdgesPenaltiesFile}

    def is_defined(self) -> bool:
        return self.penalties is not None or self.speed_multiplier is not None

    def urban_flag_required(self) -> bool:
        return isinstance(self.penalties, dict) and "urban" in self.penalties

    def run(self):
        import polars as pl

        edges: gpd.GeoDataFrame = self.input["clean_edges"].read()
        df = pl.from_pandas(edges.drop("geometry"))
        for col, param in zip(
            ("constant", "speed_multiplier"), (self.penalties, self.speed_multiplier)
        ):
            if param is None:
                # Case 0. No value given.
                df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))
            if isinstance(param, float | int):
                # Case 1. Value is number.
                df = df.with_columns(pl.lit(param, dtype=pl.Float64).alias(col))
            else:
                # Case 2. Value is dict edge_type -> value.
                from typeguard import TypeCheckError, check_type

                try:
                    check_type(param, dict[str, float])
                    df = df.with_columns(
                        pl.col("edge_type").replace_strict(param, default=None).alias(col)
                    )
                except TypeCheckError:
                    # Case 3. Value is nested dict urban -> edge_type -> value.
                    try:
                        check_type(param, dict[str, dict[str, float]])
                    except TypeCheckError:
                        pass
                    else:
                        if "urban" not in param.keys() or "rural" not in param.keys():
                            raise MetropyError("Missing keys `urban` or `rural`")
                        if "urban" not in df.columns:
                            urban_edges = self.input["urban_edges"].read()
                            df = df.join(urban_edges, on="edge_id", how="left")
                        df = df.with_columns(
                            pl.when("urban")
                            .then(pl.col("edge_type").replace_strict(param["urban"], default=None))
                            .otherwise(
                                pl.col("edge_type").replace_strict(param["rural"], default=None)
                            )
                            .alias(col)
                        )
            df = df.with_columns(pl.col("constant").cast(pl.Float64))
        df = df.select("edge_id", "constant", "speed_multiplier")
        self.output["edges_penalties"].write(df)


class FreeFlowPenaltyCoefficientsFromFileStep(Step):
    """Defines free-flow penalty coefficients at the variable-level from a data file.

    The
    [`road_network.penalty_coefficients_file`](parameters.md#road_networkpenalty_coefficients_file)
    parameter is a path to a CSV or Parquet file with the coefficients to apply on edges' variables.

    Columns are:

    - `type`: whether the penalty is additive or a multiplier of speed limit (`"additive"` or
      `"multiplicative"`)
    - `variable1`: variable to which the coefficient applies
    - `variable2`: for interaction variables, second variable to which the coefficient applies
    - `penalty`: value of the penalty

    For penalties that apply to all edges, use `"cst"` as variable.
    For categorical variables, you can use the syntax `"{variable}_{modality}"` to apply a different
    coefficient for each modality.

    For example, the following CSV file set:

    - an additive penalty of 3 seconds for all edges + 5 seconds for edges with traffic signals + 4
      seconds for urban edges with traffic signals
    - a multiplicative penalty of 0.9 for all non-residential edges and 0.9 * 0.9 for all
      residential edges

    ```csv
    type,variable1,variable2,penalty
    additive,cst,,3.0
    additive,traffic_signals,,5.0
    additive,traffic_signals,urban,4.0
    multiplicative,cst,,0.9
    multiplicative,edge_type_residential,,0.9
    ```
    """

    coef_file = PathParameter(
        "road_network.penalty_coefficients_file",
        check_file_exists=True,
        description=(
            "Path to a CSV / Parquet file with the coefficients to apply on edges' variables to "
            "compute the constant time and speed multiplier."
        ),
    )
    input_files = {"edges_variables": RoadEdgesVariablesFile}
    output_files = {"coefs": RoadEdgesPenaltyCoefficientsFile}

    def is_defined(self) -> bool:
        return self.coef_file is not None

    def run(self):
        import sys

        import polars as pl

        assert self.coef_file is not None

        var_schema = pl.read_parquet_schema(self.input["edges_variables"].complete_path)
        existing_variables = set(var_schema.keys()) - {"edge_id"}

        coefs: pl.DataFrame = read_dataframe(self.coef_file)

        # Check columns.
        has_error = False
        for col in ("type", "variable1", "variable2", "penalty"):
            if col not in coefs.columns:
                logger.error(f"Missing column in coefficients file: `{col}`")
                has_error = True
        if has_error:
            sys.exit()
        if not coefs["type"].is_in(("additive", "multiplicative")).all():
            logger.error('Column `type` can only take values `"additive"` and `"multiplicative"`')
            sys.exit()
        if coefs["variable1"].null_count() > 0:
            logger.error("There must not be any NULL value for column `variable1`")
            sys.exit()

        # Check that all defined variables are available in the variables.
        defined_variables = set(coefs["variable1"]) | set(coefs["variable2"]) - {None}
        missing_variables = defined_variables - existing_variables
        if missing_variables:
            logger.error(
                "The following variables are defined in the coefficients file but they are not "
                f"available in the edges' variables: {missing_variables}"
            )
            sys.exit()

        self.output["coefs"].write(coefs)


class EdgePenaltiesFromCoefficientsStep(Step):
    """Generates free-flow travel time penalties for each road network edge from variable-level
    coefficients.
    """

    additive_lb = FloatParameter(
        "road_network.free_flow_penalties.additive_lower_bound",
        description="Lower bound (inclusive) to the additive penalty for each edge.",
        default=0.0,
        lower_bound=0.0,
    )
    additive_ub = FloatParameter(
        "road_network.free_flow_penalties.additive_upper_bound",
        description="Upper bound (inclusive) to the additive penalty for each edge.",
        default=inf,
        lower_bound=0.0,
    )
    multiplicative_lb = FloatParameter(
        "road_network.free_flow_penalties.multiplicative_lower_bound",
        description="Lower bound (inclusive) to the multiplicative penalty for each edge.",
        default=0.0,
        lower_bound=0.0,
    )
    multiplicative_ub = FloatParameter(
        "road_network.free_flow_penalties.multiplicative_upper_bound",
        description="Upper bound (inclusive) to the multiplicative penalty for each edge.",
        default=inf,
        lower_bound=0.0,
    )
    input_files = {
        "edges_variables": RoadEdgesVariablesFile,
        "coefs": RoadEdgesPenaltyCoefficientsFile,
    }
    output_files = {"edges_penalties": RoadEdgesPenaltiesFile}

    def run(self):
        import polars as pl

        df: pl.DataFrame = self.input["edges_variables"].read()
        df = df.with_columns(cst=pl.lit(1.0, dtype=pl.Float64))
        coefs: pl.DataFrame = self.input["coefs"].read()

        df = df.with_columns(
            additive=pl.lit(0.0, dtype=pl.Float64), multiplicative=pl.lit(0.0, dtype=pl.Float64)
        )
        for row in coefs.iter_rows(named=True):
            if row["variable2"] is None:
                # Non-interaction variables.
                df = df.with_columns(
                    pl.col(row["type"]) + pl.col(row["variable1"]) * row["penalty"]
                )
            else:
                # Interaction variables.
                df = df.with_columns(
                    pl.col(row["type"])
                    + row["penalty"] * pl.col(row["variable1"]) * pl.col(row["variable2"])
                )

        df = check_bounds(df, "additive", self.additive_lb, self.additive_ub)
        df = check_bounds(df, "multiplicative", self.multiplicative_lb, self.multiplicative_ub)

        df = df.select("edge_id", constant="additive", speed_multiplier="multiplicative")
        self.output["edges_penalties"].write(df)


class EdgesFreeFlowTravelTimesStep(Step):
    """Generates free-flow travel times for each edge of the road network.

    The free-flow travel time of an edge is:

    `constant_penalty + length / speed`
    """

    speed_lb = FloatParameter(
        "road_network.min_effective_speed",
        description=(
            "Lower bound to the effective speed (in km/h) of edges, after application of "
            "multiplicative penalties."
        ),
        default=1.0,
        lower_bound=EPSILON,
    )
    speed_ub = FloatParameter(
        "road_network.max_effective_speed",
        description=(
            "Upper bound to the effective speed (in km/h) of edges, after application of "
            "multiplicative penalties."
        ),
        default=inf,
        lower_bound=EPSILON,
    )
    input_files = {
        "clean_edges": RoadEdgesCleanFile,
        "penalties": InputFile(RoadEdgesPenaltiesFile, optional=True),
    }
    output_files = {"edges_fftt": RoadEdgesFreeFlowTravelTimeFile}

    def run(self):
        import polars as pl

        edges: gpd.GeoDataFrame = self.input["clean_edges"].read()
        df = pl.from_pandas(edges.loc[:, ["edge_id", "length", "speed_limit"]])
        if self.input["penalties"].exists():
            penalties = self.input["penalties"].read()
            df = df.join(penalties, on="edge_id", how="left")
            df = df.with_columns(speed=pl.col("speed_limit") * pl.col("speed_multiplier"))
        else:
            df = df.with_columns(speed="speed_limit", constant=0.0)

        # Check bounds.
        df = check_bounds(df, "speed", self.speed_lb, self.speed_ub)

        df = df.select(
            "edge_id",
            free_flow_travel_time=pl.col("constant") + pl.col("length") / (pl.col("speed") / 3.6),
        )
        df = df.with_columns(free_flow_travel_time=pl.duration(seconds="free_flow_travel_time"))
        self.output["edges_fftt"].write(df)
