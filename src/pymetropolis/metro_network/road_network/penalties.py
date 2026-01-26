import geopandas as gpd
import numpy as np
import polars as pl
from typeguard import TypeCheckError, check_type

from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import CustomParameter

from .common import default_edge_values_validator
from .files import CleanEdgesFile, EdgesPenaltiesFile


class ExogenousEdgePenaltiesStep(Step):
    penalties = CustomParameter(
        "road_network.penalties",
        validator=default_edge_values_validator,
        description="Constant time penalty (in seconds) of edges.",
        note=(
            "The value is either a scalar value to be applied to all edges, a table "
            "`road_type -> penalty` or two tables `road_type -> penalty`, for urban and rural edges."
        ),
    )
    output_files = {"edges_penalties": EdgesPenaltiesFile}

    def is_defined(self) -> bool:
        return self.penalties is not None

    def required_files(self):
        return {"clean_edges": CleanEdgesFile}

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
