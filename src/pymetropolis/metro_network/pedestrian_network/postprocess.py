import networkx as nx
import numpy as np
from loguru import logger

from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import BoolParameter

from .files import PedestrianEdgesCleanFile, PedestrianEdgesRawFile


class PostprocessPedestrianNetworkStep(Step):
    """Performs some operations on the "raw" pedestrian network to make it suitable for simulation.

    The operations performed are:

    - Remove all parallel edges (edges with same source and target nodes), keeping only the edge
      of minimum distance. This is only done if `remove_duplicates` is `true`.
    - Keep only the largest strongly connected component of the pedestrian-network graph. This
      ensures that all origin-destination pairs are feasible. This is only done if
      `ensure_connected` is `true`.
    - Set edge ids to `1,...,n`, where `n` is the number of edges. This is only done if `reindex` is
      `true`.
    """

    remove_duplicates = BoolParameter(
        "pedestrian_network.remove_duplicates",
        default=False,
        description=(
            "Whether the duplicate edges (edges with same source and target) should be removed."
        ),
        note="If `True`, the edge with the smallest travel time is kept.",
    )
    ensure_connected = BoolParameter(
        "pedestrian_network.ensure_connected",
        default=False,
        description=(
            "Whether the network should be restricted to the largest strongly connected component "
            "of the underlying graph."
        ),
        note=(
            "If `False`, it is the user's responsibility to ensure that all origin-destination "
            "pairs are feasible."
        ),
    )
    reindex = BoolParameter(
        "pedestrian_network.reindex",
        default=False,
        description=(
            "If `true`, the edges are re-index after the postprocessing so that they are indexed "
            "from 0 to n-1."
        ),
    )

    input_files = {"raw_edges": PedestrianEdgesRawFile}
    output_files = {"clean_edges": PedestrianEdgesCleanFile}

    def run(self):
        """Reads a GeoDataFrame of edges and performs various operations to make the data ready to
        use with METROPOLIS2.
        Saves the results to the given output file.
        """
        gdf = self.input["raw_edges"].read()
        if self.remove_duplicates:
            gdf = remove_duplicates(gdf)
        if self.ensure_connected:
            gdf = select_connected(gdf)
        if self.reindex:
            gdf = reindex(gdf)
        gdf.sort_values("edge_id", inplace=True)
        self.output["clean_edges"].write(gdf)


def remove_duplicates(gdf):
    """Remove the duplicates edges, keeping in order the one with smallest distance."""
    logger.info("Removing duplicate edges")
    n0 = len(gdf)
    l0 = gdf["length"].sum()
    # Sort the dataframe.
    gdf.sort_values("length", ascending=True, inplace=True)
    # Drop duplicates.
    gdf.drop_duplicates(subset=["source", "target"], inplace=True)
    n1 = len(gdf)
    if n0 > n1:
        l1 = gdf["length"].sum()
        logger.debug(f"Number of edges removed: {n0 - n1} ({(n0 - n1) / n0:.2%})")
        logger.debug(f"Edge length removed (m): {l0 - l1:.0f} ({(l0 - l1) / l0:.2%})")
    return gdf


def select_connected(gdf):
    logger.info("Identifying strongly connected components")
    G = nx.DiGraph()
    G.add_edges_from((v[0], v[1]) for v in gdf[["source", "target"]].values)
    # Keep only the nodes of the largest strongly connected component.
    nodes = max(nx.strongly_connected_components(G), key=len)
    if len(nodes) < G.number_of_nodes():
        logger.warning(
            f"Discarding {G.number_of_nodes() - len(nodes)} nodes disconnected from the "
            "largest graph component"
        )
        n0 = len(gdf)
        l0 = gdf["length"].sum()
        gdf = gdf.loc[gdf["source"].isin(nodes) & gdf["target"].isin(nodes)].copy()
        n1 = len(gdf)
        l1 = gdf["length"].sum()
        logger.debug(f"Number of edges removed: {n0 - n1} ({(n0 - n1) / n0:.2%})")
        logger.debug(f"Edge length removed (m): {l0 - l1:.0f} ({(l0 - l1) / l0:.2%})")
    return gdf


def reindex(gdf):
    gdf["edge_id"] = np.arange(1, len(gdf) + 1, dtype=np.uint64)
    return gdf
