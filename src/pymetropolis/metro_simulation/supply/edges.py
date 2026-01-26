import geopandas as gpd
import polars as pl

from pymetropolis.metro_common.utils import time_to_seconds_since_midnight_pl
from pymetropolis.metro_network.road_network import (
    CleanEdgesFile,
    EdgesCapacitiesFile,
    EdgesPenaltiesFile,
)
from pymetropolis.metro_pipeline.steps import MetroStep

from .files import MetroEdgesFile


class WriteMetroEdgesStep(MetroStep):
    output_files = {"metro_edges": MetroEdgesFile}

    def required_files(self):
        return {"clean_edges": CleanEdgesFile}

    def optional_files(self):
        return {"capacities": EdgesCapacitiesFile, "penalties": EdgesPenaltiesFile}

    def run(self):
        edges: gpd.GeoDataFrame = self.input["clean_edges"].read()
        columns = ["edge_id", "source", "target", "length", "speed_limit", "lanes"]
        df = pl.from_pandas(edges.loc[:, columns])
        df = df.select(
            "edge_id",
            "source",
            "target",
            "length",
            "lanes",
            speed=pl.col("speed_limit") / 3.6,
            overtaking=pl.lit(True),
        )
        if self.input["capacities"].exists():
            capacities: pl.DataFrame = self.input["capacities"].read()
            df = (
                df.join(capacities, on="edge_id", how="left")
                .with_columns(
                    bottleneck_flow=pl.col("capacity") / 3600.0,
                    bottleneck_flows=pl.col("capacities").list.eval(pl.element() / 3600.0),
                    bottleneck_times=pl.col("times").list.eval(
                        time_to_seconds_since_midnight_pl(pl.element())
                    ),
                )
                .drop("capacity", "capacities", "times")
            )
            if df["bottleneck_flows"].is_null().all():
                df = df.drop("bottleneck_flows", "bottleneck_times")
        if self.input["penalties"].exists():
            penalties: pl.DataFrame = self.input["penalties"].read()
            df = df.join(penalties, on="edge_id", how="left").rename(
                {"constant": "constant_travel_time"}
            )
        self.output["metro_edges"].write(df)
