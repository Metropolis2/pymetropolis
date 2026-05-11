from typing import TYPE_CHECKING

from pymetropolis.metro_common.utils import pl_duration_to_seconds
from pymetropolis.metro_network.road_network import (
    RoadEdgesCapacitiesFile,
    RoadEdgesCleanFile,
    RoadEdgesPenaltiesFile,
)
from pymetropolis.metro_network.road_network.files import RoadEdgesPrimaryFlagFile
from pymetropolis.metro_pipeline.steps import InputFile, Step

from .files import MetroEdgesFile, MetroVehicleTypesFile

if TYPE_CHECKING:
    import geopandas as gpd


class WriteMetroEdgesStep(Step):
    """Generates the input edges file for the Metropolis-Core simulation."""

    input_files = {
        "clean_edges": RoadEdgesCleanFile,
        "capacities": InputFile(RoadEdgesCapacitiesFile, optional=True),
        "penalties": InputFile(RoadEdgesPenaltiesFile, optional=True),
        "primary_flags": InputFile(RoadEdgesPrimaryFlagFile, optional=True),
        # MetroVehicleTypesFile is added as a dependency so that Edges are written only if vehicle
        # types are properly defined (Metropolis-Core cannot run with only edges).
        "vehicle_types": MetroVehicleTypesFile,
    }
    output_files = {"metro_edges": MetroEdgesFile}

    def run(self):
        import polars as pl

        edges: gpd.GeoDataFrame = self.input["clean_edges"].read()
        columns = ["edge_id", "source", "target", "length", "speed_limit", "lanes", "hov_lanes"]
        df = pl.from_pandas(edges.loc[:, columns])
        # Cast all ids to String. This prevents issues when the ids are integer but are cast to
        # String to handle parallel edges or HOV edges (since origin / destination ids also need to
        # be cast to String in this case).
        df = df.with_columns(
            pl.col("edge_id").cast(pl.String),
            pl.col("source").cast(pl.String),
            pl.col("target").cast(pl.String),
        )
        if self.input["primary_flags"].exists():
            primary_flags = self.input["primary_flags"].read()
            df = df.join(primary_flags.filter("primary"), on="edge_id", how="semi")
        df = df.with_columns(original_id=pl.col("edge_id"))
        st_counts = df.group_by("source", "target").len()
        if st_counts["len"].max() > 1:
            # Add dummy nodes and edges to prevent parallel edges.
            # If there are two edges, 1 and 2, from node 10 to node 11:
            # - Set their target node to "10-11-dummy-0" and "10-11-dummy-1", respectively.
            # - Add two edges with id "1-dummy" and "2-dummy", with source "10-11-dummy-0" and
            #   "10-11-dumy-1", with target "11" and "11", and with length 0.
            parallel_edges = (
                df.join(st_counts.filter(pl.col("len") > 1), on=["source", "target"], how="semi")
                .with_columns(st_idx=pl.int_range(pl.len()).over("source", "target"))
                .with_columns(
                    dummy_node=pl.concat_str(
                        "source", pl.lit("-"), "target", pl.lit("-dummy-"), "st_idx"
                    )
                )
                .drop("st_idx")
            )
            dummy_edges = parallel_edges.with_columns(
                edge_id=pl.concat_str(pl.col("edge_id"), pl.lit("-dummy")),
                # Set original_id to None so that no bottleneck capacity will be attached to this
                # edge.
                original_id=None,
                source="dummy_node",
                length=0.0,
            ).drop("dummy_node")
            df = pl.concat(
                (
                    # Edges which are NOT parallel.
                    df.join(parallel_edges, on="edge_id", how="anti"),
                    # Parallel edges (with target node modified).
                    parallel_edges.with_columns(target="dummy_node").drop("dummy_node"),
                    # Dummy edges to connect modified target node to actual target node.
                    dummy_edges,
                )
            )
        if df["hov_lanes"].max() > 0.0:
            # Add "-hov" to the id of each hov edge.
            hov_edges = df.filter(pl.col("hov_lanes") > 0).with_columns(
                edge_id=pl.concat_str("edge_id", pl.lit("-hov")),
                target=pl.concat_str("source", pl.lit("-"), "target", pl.lit("-dummy-hov")),
                lanes="hov_lanes",
            )
            # Create dummy edges to prevent the parallel edges problem.
            dummy_edges = df.filter(pl.col("hov_lanes") > 0).with_columns(
                edge_id=pl.concat_str(pl.col("edge_id"), pl.lit("-dummy-hov")),
                # Set original_id to None so that no bottleneck capacity will be attached to this
                # edge.
                original_id=None,
                source=pl.concat_str("source", pl.lit("-"), "target", pl.lit("-dummy-hov")),
                length=0.0,
            )
            # Remove HOV lanes from the actual number of lanes.
            # And filter out hov-only edges.
            df = df.filter(pl.col("lanes") > 0).with_columns(
                lanes=pl.col("lanes") - pl.col("hov_lanes")
            )
            df = pl.concat((df, hov_edges, dummy_edges))
        df = df.select(
            "edge_id",
            "original_id",
            "source",
            "target",
            "length",
            "lanes",
            speed=pl.col("speed_limit") / 3.6,
            overtaking=pl.lit(True),
        )
        if self.input["capacities"].exists():
            capacities: pl.DataFrame = self.input["capacities"].read()
            # The join is done on the `original_id` column so that both normal edges and HOV edges
            # are attached the correct capacity.
            df = (
                df.join(
                    capacities.select(
                        "capacity",
                        "capacities",
                        "times",
                        original_id=pl.col("edge_id").cast(pl.String),
                    ),
                    on="original_id",
                    how="left",
                )
                .with_columns(
                    bottleneck_flow=pl.col("capacity") / 3600.0,
                    bottleneck_flows=pl.col("capacities").list.eval(pl.element() / 3600.0),
                    bottleneck_times=pl.col("times").list.eval(
                        pl_duration_to_seconds(pl.element())
                    ),
                )
                .drop("capacity", "capacities", "times")
            )
            if df["bottleneck_flows"].is_null().all():
                df = df.drop("bottleneck_flows", "bottleneck_times")
        if self.input["penalties"].exists():
            penalties: pl.DataFrame = self.input["penalties"].read()
            df = df.join(
                penalties.select("constant", original_id=pl.col("edge_id").cast(pl.String)),
                left_on="original_id",
                right_on=pl.col("edge_id").cast(pl.String),
                how="left",
            ).rename({"constant": "constant_travel_time"})
        df = df.drop("original_id").sort("source", "target")
        self.output["metro_edges"].write(df)
