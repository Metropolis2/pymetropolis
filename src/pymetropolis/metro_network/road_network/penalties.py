import geopandas as gpd
import numpy as np
import polars as pl
from typeguard import TypeCheckError, check_type

from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import CustomParameter
from pymetropolis.metro_pipeline.steps import InputFile

from .common import default_edge_values_validator
from .files import CleanEdgesFile, EdgesFreeFlowTravelTimeFile, EdgesPenaltiesFile


class ExogenousEdgePenaltiesStep(Step):
    """Generates travel time penalties for the road network edges, from exogenous values.

    The penalties can be:

    - constant over edges
    - constant by road type
    - constant by combinations of road type and urban flag
    """

    penalties = CustomParameter(
        "road_network.penalties",
        validator=default_edge_values_validator,
        description="Constant time penalty (in seconds) of edges.",
        validator_description=(
            "float (constant penalty for all edges), table with road types as keys and penalties"
            ' as values, or table with "urban" and "rural" as keys and `road_type->value` tables as'
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
    input_files = {"clean_edges": CleanEdgesFile}
    output_files = {"edges_penalties": EdgesPenaltiesFile}

    def is_defined(self) -> bool:
        return self.penalties is not None

    def run(self):
        penalties = self.penalties
        edges: gpd.GeoDataFrame = self.input["clean_edges"].read()
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
                    edges.loc[mask, "constant"] = edges.loc[mask, "road_type"].map(
                        penalties["urban"]
                    )
                    mask = ~edges["urban"]
                    edges.loc[mask, "constant"] = edges.loc[mask, "road_type"].map(
                        penalties["rural"]
                    )
        df = pl.from_pandas(edges[["edge_id", "constant"]])
        df = df.with_columns(pl.col("constant").cast(pl.Float64))
        self.output["edges_penalties"].write(df)


class EdgesFreeFlowTravelTimesStep(Step):
    """Generates free-flow travel times for each edge of the road network.

    The free-flow travel time of an edge is:

    `constant_penalty + length / speed`
    """

    input_files = {
        "clean_edges": CleanEdgesFile,
        "penalties": InputFile(EdgesPenaltiesFile, optional=True),
    }
    output_files = {"edges_fftt": EdgesFreeFlowTravelTimeFile}

    def run(self):
        edges: gpd.GeoDataFrame = self.input["clean_edges"].read()
        df = pl.from_pandas(edges.loc[:, ["edge_id", "length", "speed_limit"]])
        df = df.select(
            "edge_id", free_flow_travel_time=pl.col("length") / (pl.col("speed_limit") / 3.6)
        )
        if self.input["penalties"].exists():
            penalties = self.input["penalties"]
            df = df.join(penalties, on="edge_id", how="left")
            df = df.select(
                "edge_id",
                free_flow_travel_time=pl.col("free_flow_travel_time") + pl.col("constant"),
            )
        df = df.with_columns(free_flow_travel_time=pl.duration(seconds="free_flow_travel_time"))
        self.output["edges_fftt"].write(df)
