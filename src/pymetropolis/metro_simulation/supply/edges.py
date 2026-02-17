import geopandas as gpd
import polars as pl

from pymetropolis.metro_common.utils import time_to_seconds_since_midnight_pl
from pymetropolis.metro_network.road_network import (
    CleanEdgesFile,
    EdgesCapacitiesFile,
    EdgesPenaltiesFile,
)
from pymetropolis.metro_pipeline.steps import InputFile, Step

from .files import MetroEdgesFile


class WriteMetroEdgesStep(Step):
    """Generates the input edges file for the Metropolis-Core simulation."""

    input_files = {
        "clean_edges": CleanEdgesFile,
        "capacities": InputFile(EdgesCapacitiesFile, optional=True),
        "penalties": InputFile(EdgesPenaltiesFile, optional=True),
    }
    output_files = {"metro_edges": MetroEdgesFile}

    def run(self):
        edges: gpd.GeoDataFrame = self.input["clean_edges"].read()
        columns = ["edge_id", "source", "target", "length", "speed_limit", "lanes", "hov_lanes"]
        df = pl.from_pandas(edges.loc[:, columns])
        if df["hov_lanes"].max() > 0.0:
            df = df.with_columns(
                pl.col("edge_id").cast(pl.String),
                pl.col("source").cast(pl.String),
                pl.col("target").cast(pl.String),
            )
            # Add "-hov" to the id of each hov edge.
            hov_edges = df.filter(pl.col("hov_lanes") > 0).with_columns(
                new_edge_id=pl.concat_str("edge_id", pl.lit("-hov")),
                target=pl.concat_str("source", pl.lit("-"), "target", pl.lit("-dummy")),
                lanes="hov_lanes",
            )
            # Create dummy edges to prevent the parallel edges problem.
            dummy_edges = df.filter(pl.col("hov_lanes") > 0).with_columns(
                # Set edge_id to None so that no bottleneck capacity will be attached to this edge.
                edge_id=None,
                new_edge_id=pl.concat_str(pl.col("edge_id"), pl.lit("-dummy")),
                source=pl.concat_str("source", pl.lit("-"), "target", pl.lit("-dummy")),
                length=0.0,
            )
            breakpoint()
            # Remove HOV lanes from the actual number of lanes.
            # And filter out hov-only edges.
            df = df.with_columns(
                new_edge_id="edge_id", lanes=pl.col("lanes") - pl.col("hov_lanes")
            ).filter(pl.col("lanes") > 0)
            df = pl.concat((df, hov_edges, dummy_edges))
        else:
            df = df.with_columns(new_edge_id="edge_id")
        df = df.select(
            edge_id="new_edge_id",
            source="source",
            target="target",
            length="length",
            lanes="lanes",
            speed=pl.col("speed_limit") / 3.6,
            overtaking=pl.lit(True),
        )
        if self.input["capacities"].exists():
            capacities: pl.DataFrame = self.input["capacities"].read()
            # The join will attach the capacities to both the normal edges and the HOV edges (if
            # any).
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
            # The join will attach the penalties to both the normal edges and the HOV edges (if
            # any).
            df = df.join(penalties, on="edge_id", how="left").rename(
                {"constant": "constant_travel_time"}
            )
        df = df.sort("source", "target")
        self.output["metro_edges"].write(df)
