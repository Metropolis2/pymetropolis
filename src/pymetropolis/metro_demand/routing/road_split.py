from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_demand.routing.files import (
    NonPrimaryCarTrips,
    PrimaryCarTripsAccessEgressFile,
    TripsCarFreeFlowTravelTimesFile,
)
from pymetropolis.metro_network.functions import get_largest_strongly_connected_component_nodes
from pymetropolis.metro_network.road_network.files import (
    RoadEdgesCleanFile,
    RoadEdgesFreeFlowTravelTimeFile,
    RoadEdgesPrimaryFlagFile,
)
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import BoolParameter, ListParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_pipeline.types import String

if TYPE_CHECKING:
    import polars as pl


def find_first_last_primary(routes: pl.DataFrame, primary_edges: set) -> pl.DataFrame:
    """From a DataFrame with columns `trip_id` and `route` (lists of edge ids), returns a DataFrame
    with columns:

    - `trip_id`
    - `first_idx_primary`: index of the first observed primary edge (or NULL if there is none)
    - `last_idx_primary`: index of the last observed primary edge (or NULL if there is none)

    Notes:
    - Indices are 1-based.
    - Trips with only secondary edges (or no edge at all) are dropped.
    """
    # We use duckdb instead of polars to limit RAM use (the DataFrame can be very large with many
    # trips and long routes).
    import duckdb

    con = duckdb.connect()
    con.register("routes", routes)
    return con.execute(
        """
        SELECT
            trip_id,
            MIN(idx)  AS first_idx_primary,
            MAX(idx)  AS last_idx_primary
        FROM (
            SELECT
                trip_id,
                UNNEST(route) AS edge_id,
                GENERATE_SUBSCRIPTS(route, 1) AS idx
            FROM routes
        )
        WHERE edge_id = ANY($1)
        GROUP BY trip_id
    """,
        [list(primary_edges)],
    ).pl()


def find_primary_edges(routes: pl.DataFrame, primary_edges: set, i=0) -> set:
    """Recursively adds edges to the primary network when they are "in the middle" of the primary
    parts."""
    import polars as pl

    logger.debug(f"Iteration {i}")
    # Compute the set of secondary edges taken in-between the primary part.
    primary_idx = find_first_last_primary(routes, primary_edges)
    secondary_edges_in_middle = set(
        routes.lazy()
        .join(primary_idx.lazy(), on="trip_id")
        .select(
            edge_id=pl.col("route").list.slice(
                pl.col("first_idx_primary") - 1,
                pl.col("last_idx_primary") - pl.col("first_idx_primary") + 1,
            )
        )
        .explode("edge_id")
        .filter(pl.col("edge_id").is_in(primary_edges).not_())
        .collect(engine="streaming")
        .to_series()  # ty: ignore[unresolved-attribute]
    )
    if secondary_edges_in_middle:
        logger.debug(f"Adding {len(secondary_edges_in_middle):,} secondary edges to primary graph")
        primary_edges |= secondary_edges_in_middle
        return find_primary_edges(routes, primary_edges, i + 1)
    logger.debug(f"Total number of edges in the primary graph: {len(primary_edges):,}")
    return primary_edges


def find_connections(routes: pl.DataFrame, edges: pl.DataFrame, primary_edges: set):
    """Creates `access_*` and `egress_*` columns from the free-flow routes and the set of primary
    edges."""
    import polars as pl

    primary_idx = find_first_last_primary(routes, primary_edges)
    df = routes.join(primary_idx, on="trip_id", how="left")
    primary_trips = (
        df.lazy()
        .filter(pl.col("first_idx_primary").is_not_null())
        .with_columns(
            first_primary_edge=pl.col("route").list.get(pl.col("first_idx_primary") - 1),
            last_primary_edge=pl.col("route").list.get(pl.col("last_idx_primary") - 1),
            access_path=pl.col("route").list.slice(0, pl.col("first_idx_primary") - 1),
            egress_path=pl.col("route").list.slice(pl.col("last_idx_primary")),
        )
        .with_columns(
            access_time=pl.col("access_path")
            .list.eval(
                pl.element().replace_strict(edges["edge_id"], edges["free_flow_travel_time"])
            )
            .list.sum(),
            egress_time=pl.col("egress_path")
            .list.eval(
                pl.element().replace_strict(edges["edge_id"], edges["free_flow_travel_time"])
            )
            .list.sum(),
            access_length=pl.col("access_path")
            .list.eval(pl.element().replace_strict(edges["edge_id"], edges["length"]))
            .list.sum(),
            egress_length=pl.col("egress_path")
            .list.eval(pl.element().replace_strict(edges["edge_id"], edges["length"]))
            .list.sum(),
            access_node=pl.col("first_primary_edge").replace_strict(
                edges["edge_id"], edges["source"]
            ),
            egress_node=pl.col("last_primary_edge").replace_strict(
                edges["edge_id"], edges["target"]
            ),
        )
        .select(
            "trip_id",
            "access_node",
            "access_path",
            "access_time",
            "access_length",
            "egress_node",
            "egress_path",
            "egress_time",
            "egress_length",
        )
        .collect()
    )
    secondary_trips = (
        df.lazy()
        .filter(pl.col("first_idx_primary").is_null())
        .select(
            "trip_id",
            "free_flow_travel_time",
            path="route",
            path_length=pl.col("route")
            .list.eval(pl.element().replace_strict(edges["edge_id"], edges["length"]))
            .list.sum(),
        )
        .collect()
    )
    return primary_trips, secondary_trips


class RoadNetworkPrimaryEdgesStep(Step):
    """Identifies the edges which are part of the "primary" road network.

    Only primary edges are simulated in Metropolis-Core, whereas the secondary edges are supposed to
    always be under free-flow conditions.
    By default, all edges are considered "primary".

    You can use the [`secondary_types`](parameters.md#road_networksecondary_types) parameter to
    define the list of edges' types that should be considered "secondary".
    For example, setting `secondary_types` to `["residential"]` excludes residential edges from the
    primary network, which can be useful as these edges are rarely congested but often represent a
    large portion of the road network.

    When secondary edges are defined, road trips are split in at most three parts:

    1. A sequence of secondary edges (access).
    2. A sequence of primary edges (simulated in Metropolis-Core).
    3. A sequence of secondary edges (egress).

    Since only the primary part of the trip is simulated, secondary edges cannot appear in the
    middle of the primary part.
    If the [`ensure_primary_connected`](parameters.md#road_networkensure_primary_connected)
    parameter is set to `true`, Pymetropolis will add secondary edges to the primary network to
    ensure that the fastest free-flow route for each trip can be completed without requiring more
    than three parts.
    """

    secondary_types = ListParameter(
        "road_network.secondary_types",
        inner=String(),
        default=[],
        description='List of edges\' types that are part of the "secondary" road network.',
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
            'both after the first occurrence of a "primary" edge and before the last occurrence of '
            'a "primary" edge.'
        ),
    )
    input_files = {
        "edges": RoadEdgesCleanFile,
        "car_ff_routes": InputFile(
            TripsCarFreeFlowTravelTimesFile,
            when=lambda inst: inst.secondary_types and inst.ensure_primary_connected,
            when_doc="if `secondary_types` is not empty and `ensure_primary_connected` is `true`",
        ),
    }
    output_files = {"edges_primary": RoadEdgesPrimaryFlagFile}

    def run(self):
        import polars as pl

        edges_gdf = self.input["edges"].read()
        edges = pl.from_pandas(edges_gdf.loc[:, ["edge_id", "edge_type", "source", "target"]])
        if self.secondary_types:
            df = edges.select(
                "edge_id", primary=pl.col("edge_type").is_in(self.secondary_types).not_()
            )
        else:
            # Default case: all edges are primary.
            df = edges.select("edge_id", primary=True)
        if not df["primary"].all() and self.ensure_primary_connected:
            routes = self.input["car_ff_routes"].read().select("trip_id", route="free_flow_route")
            primary_edges = set(df.filter("primary")["edge_id"])
            primary_edges = find_primary_edges(routes, primary_edges)
            edges = edges.with_columns(primary=pl.col("edge_id").is_in(primary_edges))
            # Select the largest strongly connected component.
            # Some patches of edges can be disconnected and are not re-connected to the main part
            # since no trip is starting from them.
            nodes = get_largest_strongly_connected_component_nodes(
                edges.filter("primary").select("source", "target")
            )
            n0 = len(primary_edges)
            df = edges.select(
                "edge_id",
                primary=pl.col("primary")
                .and_(pl.col("source").is_in(nodes))
                .and_(pl.col("target").is_in(nodes)),
            )
            n1 = df["primary"].sum()
            if n1 < n0:
                logger.warning(
                    f"Discarding {n0 - n1} primary edges ({(n0 - n1) / n0:.2%}) disconnected from "
                    "the largest graph component."
                )
        if not df["primary"].sum():
            raise MetropyError(
                "There is no edge in the primary network. "
                "Are the `primary_types` properly specified?"
            )
        self.output["edges_primary"].write(df)


class CarAccessEgressStep(Step):
    """Identifies the access and egress parts of car trips based on the primary road network.

    For each car trip, this step determines:

    - **Access part**: The sequence of edges taken *before* the first primary edge in the trip.
    - **Egress part**: The sequence of edges taken *after* the last primary edge in the trip.

    The access and egress parts are derived from the free-flow route of the trip and the set of
    primary edges. Trips that consist solely of primary edges or solely of secondary edges will not
    have access or egress parts.

    The output includes the following information for each trip:

    - `access_node`: The node where the primary part of the trip begins.
    - `access_path`: The list of edges in the access part.
    - `access_time`: The total free-flow travel time for the access part.
    - `access_length`: Length of the access part (meters).
    - `egress_node`: The node where the primary part of the trip ends.
    - `egress_path`: The list of edges in the egress part.
    - `egress_time`: The total free-flow travel time for the egress part.
    - `egress_length`: Length of the egress part (meters).
    """

    input_files = {
        "edges": RoadEdgesCleanFile,
        "primary_flags": RoadEdgesPrimaryFlagFile,
        "edges_fftt": RoadEdgesFreeFlowTravelTimeFile,
        "car_ff_routes": TripsCarFreeFlowTravelTimesFile,
    }
    output_files = {
        "primary_trips": PrimaryCarTripsAccessEgressFile,
        "secondary_trips": NonPrimaryCarTrips,
    }

    def run(self):
        import polars as pl

        edges_gdf = self.input["edges"].read()
        edges = pl.from_pandas(edges_gdf.loc[:, ["edge_id", "source", "target", "length"]])
        edges_fftt = self.input["edges_fftt"].read()
        edges = edges.join(edges_fftt, on="edge_id", how="left")
        primary_flags = self.input["primary_flags"].read()
        primary_edges = set(primary_flags.filter("primary")["edge_id"])
        routes = (
            self.input["car_ff_routes"]
            .read()
            .select(
                "trip_id", route="free_flow_route", free_flow_travel_time="free_flow_travel_time"
            )
        )
        primary_trips, secondary_trips = find_connections(routes, edges, primary_edges)
        self.output["primary_trips"].write(primary_trips)
        self.output["secondary_trips"].write(secondary_trips)
