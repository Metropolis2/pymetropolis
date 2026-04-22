import polars as pl
from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_demand.routing.files import TripsCarFreeFlowTravelTimesFile
from pymetropolis.metro_network.road_network.files import (
    RoadEdgesCleanFile,
    RoadEdgesPrimaryFlagFile,
)
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import BoolParameter, ListParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_pipeline.types import String


def find_primary_edges(routes: pl.DataFrame, primary_edges: set, i=0):
    logger.debug(f"Iteration {i}")
    # Compute the set of secondary edges taken in-between the primary part.
    secondary_edges_in_middle = set(
        routes.lazy()
        .explode("route")
        .with_columns(
            idx=pl.int_range(pl.len()).over("id"), is_primary=pl.col("route").is_in(primary_edges)
        )
        # Find index of first and last primary edge of each route.
        .with_columns(
            first_idx_primary=pl.col("idx").filter("is_primary").first().over("id"),
            last_idx_primary=pl.col("idx").filter("is_primary").last().over("id"),
        )
        # Filter secondary edges after the first primary edge and before the last primary edge.
        .filter(
            pl.col("idx") > pl.col("first_idx_primary"),
            pl.col("idx") < pl.col("last_idx_primary"),
            pl.col("is_primary").not_(),
        )
        .select("route")
        .collect(engine="streaming")
        .to_series()  # ty: ignore[unresolved-attribute]
    )
    if secondary_edges_in_middle:
        logger.debug(f"Adding {len(secondary_edges_in_middle):,} secondary edges to primary graph")
        primary_edges |= secondary_edges_in_middle
        return find_primary_edges(routes, primary_edges, i + 1)
    logger.debug(f"Total number of edges in the primary graph: {len(primary_edges):,}")
    return primary_edges


class RoadNetworkPrimaryEdgesStep(Step):
    """Identifies the edges which are part of the "primary" road network."""

    primary_types = ListParameter(
        "road_network.primary_types",
        inner=String(),
        default=[],
        description='List of edges\' types are always part of the "primary" road network.',
        note='By default, all edges are part of the "primary" road network.',
    )
    ensure_primary_connected = BoolParameter(
        "road_network.ensure_primary_connected",
        default=True,
        description=(
            "When `true`, add some secondary edges to the primary network to ensure that free-flow "
            "routes are primary-connected."
        ),
        note=(
            'A free-flow route is "primary-connected" if there are no "secondary" edges which are '
            'both after the first occurence of a "primary" edge and before the last occurence of a '
            '"primary" edge.'
        ),
    )
    input_files = {
        "edges": RoadEdgesCleanFile,
        "car_ff_routes": InputFile(
            TripsCarFreeFlowTravelTimesFile,
            when=lambda inst: inst.ensure_primary_connected,
            when_doc="`ensure_primary_connected` is `true`",
        ),
    }
    output_files = {"edges_primary": RoadEdgesPrimaryFlagFile}

    def run(self):
        edges_gdf = self.input["edges"].read()
        edges = pl.from_pandas(edges_gdf.loc[:, ["edge_id", "edge_type"]])
        if self.primary_types:
            df = edges.select("edge_id", primary=pl.col("edge_type").is_in(self.primary_types))
        else:
            # Default case: all edges are primary.
            df = edges.select("edge_id", primary=True)
        if self.ensure_primary_connected:
            routes = (
                self.input["car_ff_routes"].read().select(id="trip_id", route="free_flow_route")
            )
            primary_edges = set(df.filter("primary")["edge_id"])
            find_primary_edges(routes, primary_edges)
        if not df["primary"].sum():
            raise MetropyError(
                "There is no edge in the primary network. "
                "Are the `primary_types` properly specified?"
            )
        self.output["edges_primary"].write(df)
