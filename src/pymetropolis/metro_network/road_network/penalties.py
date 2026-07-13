from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_common.io import read_dataframe
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import CustomParameter, PathParameter
from pymetropolis.metro_pipeline.steps import InputFile

from .common import default_edge_values_validator
from .files import (
    RoadEdgesCleanFile,
    RoadEdgesFreeFlowTravelTimeFile,
    RoadEdgesPenaltiesFile,
    RoadEdgesUrbanFlagFile,
    RoadEdgesVariablesFile,
)

if TYPE_CHECKING:
    import geopandas as gpd


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


class EdgePenaltiesFromCoefficientsStep(Step):
    """TODO

    columns:

    ```csv
    type,coefficient1,coefficient2,penalty
    constant,base,,3.0
    speed_multiplier,base,,0.9
    constant,traffic_signals,,5.0
    constant,traffic_signals,urban,4.0
    speed_multiplier,edge_type_residential,,0.9
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
    output_files = {"edges_penalties": RoadEdgesPenaltiesFile}

    def is_defined(self) -> bool:
        return self.coef_file is not None

    def run(self):
        import sys

        import polars as pl

        df: pl.DataFrame = self.input["edges_variables"].read()
        df = df.with_columns(base=pl.lit(1.0, dtype=pl.Float64))
        coefs: pl.DataFrame = read_dataframe(self.coef_file)

        # Check columns.
        has_error = False
        for col in ("type", "coefficient1", "coefficient2", "penalty"):
            if col not in coefs.columns:
                logger.error(f"Missing column in coefficients file: `{col}`")
                has_error = True
        if has_error:
            sys.exit()
        if not coefs["type"].is_in(("constant", "speed_multiplier")).all():
            logger.error('Column `type` can only take values `"constant"` and `"speed_multiplier"`')
            sys.exit()
        if coefs["coefficient1"].null_count() > 0:
            logger.error("There must not be any NULL value for column `coefficient1`")
            sys.exit()

        # Check that all defined coefficients are available in the variables.
        defined_coefs = set(coefs["coefficient1"]) | set(coefs["coefficient2"]) - {None}
        existing_coefs = set(df.columns) - {"edge_id"}
        missing_coefs = defined_coefs - existing_coefs
        if missing_coefs:
            logger.error(
                "The following coeficients are defined in the coefficients file but they are not "
                f"available in the edges'variables: {missing_coefs}"
            )
            sys.exit()

        df = df.with_columns(
            constant=pl.lit(0.0, dtype=pl.Float64), speed_multiplier=pl.lit(0.0, dtype=pl.Float64)
        )
        for row in coefs.iter_rows(named=True):
            if row["coefficient2"] is None:
                # Non-interaction variables.
                df = df.with_columns(
                    pl.col(row["type"]) + pl.col(row["coefficient1"]) * row["penalty"]
                )
            else:
                # Interaction variables.
                df = df.with_columns(
                    pl.col(row["type"])
                    + row["penalty"] * pl.col(row["coefficient1"]) * pl.col(row["coefficient2"])
                )

        df = df.select("edge_id", "constant", "speed_multiplier")
        self.output["edges_penalties"].write(df)


class EdgesFreeFlowTravelTimesStep(Step):
    """Generates free-flow travel times for each edge of the road network.

    The free-flow travel time of an edge is:

    `constant_penalty + length / speed`
    """

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
        df = df.select(
            "edge_id",
            free_flow_travel_time=pl.col("constant") + pl.col("length") / (pl.col("speed") / 3.6),
        )
        df = df.with_columns(free_flow_travel_time=pl.duration(seconds="free_flow_travel_time"))
        self.output["edges_fftt"].write(df)
