import geopandas as gpd
import networkx as nx
import numpy as np

from pymetropolis.metro_common.errors import error_context
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import BoolParameter, CustomParameter, FloatParameter

from .common import default_edge_values_validator
from .files import CleanEdgesFile, RawEdgesFile

EPSILON = np.finfo(float).eps


class PostprocessRoadNetworkStep(Step):
    min_nb_lanes = FloatParameter(
        "road_network_postprocess.min_nb_lanes",
        default=1.0,
        description="Minimum number of lanes allowed on edges.",
    )
    min_speed_limit = FloatParameter(
        "road_network_postprocess.min_speed_limit",
        default=EPSILON,
        description="Minimum speed limit allowed on edges (in km/h).",
    )
    min_length = FloatParameter(
        "road_network_postprocess.min_length",
        default=0.0,
        description="Minimum length allowed on edges (in meters).",
    )
    remove_duplicates = BoolParameter(
        "road_network_postprocess.remove_duplicates",
        default=False,
        description="Whether the duplicate edges (edges with same source and target) should be removed.",
        note="If `True`, the edge with the smallest travel time is kept.",
    )
    ensure_connected = BoolParameter(
        "road_network_postprocess.ensure_connected",
        default=False,
        description=(
            "Whether the network should be restricted to the largest strongly connected component of "
            "the underlying graph."
        ),
        note=(
            "If `False`, it is the user's responsibility to ensure that all origin-destination pairs "
            "are feasible."
        ),
    )
    reindex = BoolParameter(
        "road_network_postprocess.reindex",
        default=False,
        description=(
            "If `true`, the edges are re-index after the postprocessing so that they are indexed from "
            "0 to n-1."
        ),
    )
    default_speed_limit = CustomParameter(
        "road_network_postprocess.default_speed_limit",
        validator=default_edge_values_validator,
        description="Default speed limit (in km/h) to use for edges with no specified value.",
        note=(
            "The value is either a scalar value to be applied to all edges with no specified value, a "
            "table `road_type -> speed_limit` or two tables `road_type -> speed_limit`, for urban and "
            "rural edges."
        ),
    )
    default_nb_lanes = CustomParameter(
        "road_network_postprocess.default_nb_lanes",
        validator=default_edge_values_validator,
        default=1.0,
        description="Default number of lanes to use for edges with no specified value.",
        note=(
            "The value is either a scalar value to be applied to all edges with no specified value, a "
            "table `road_type -> nb_lanes` or two tables `road_type -> nb_lanes`, for urban and rural "
            "edges."
        ),
    )
    output_files = {"clean_edges": CleanEdgesFile}

    def required_files(self):
        return {"raw_edges": RawEdgesFile}

    def run(self):
        """Reads a GeoDataFrame of edges and performs various operations to make the data ready to
        use with METROPOLIS2.
        Saves the results to the given output file.
        """
        gdf = self.input["raw_edges"].read()
        gdf = set_default_values(
            gdf,
            default_speed_limit=self.default_speed_limit,
            default_nb_lanes=self.default_nb_lanes,
        )
        if self.remove_duplicates:
            gdf = remove_duplicates(gdf)
        if self.ensure_connected:
            gdf = select_connected(gdf)
        if self.reindex:
            gdf = reindex(gdf)
        gdf = check(
            gdf,
            min_nb_lanes=self.min_nb_lanes,
            min_speed_limit=self.min_speed_limit,
            min_length=self.min_length,
        )
        gdf.sort_values("edge_id", inplace=True)
        self.output["clean_edges"].write(gdf)


@error_context("Failed to set default values of edges")
def set_default_values(gdf, default_speed_limit: float | dict, default_nb_lanes: float | dict):
    # Set default for bool columns (default is always False).
    for col in ("toll", "roundabout", "give_way", "stop", "traffic_signals", "urban"):
        if col not in gdf.columns:
            gdf[col] = False
        else:
            gdf[col] = gdf[col].fillna(False)
    # Set default speed limits.
    if "speed_limit" not in gdf.columns:
        gdf["speed_limit"] = np.nan
    gdf["default_speed_limit"] = gdf["speed_limit"].isna()
    if isinstance(default_speed_limit, float):
        gdf["speed_limit"] = gdf["speed_limit"].fillna(default_speed_limit)
    elif isinstance(default_speed_limit, dict):
        if "urban" in default_speed_limit.keys() and "rural" in default_speed_limit.keys():
            mask = gdf["urban"] & gdf["speed_limit"].isna()
            gdf.loc[mask, "speed_limit"] = gdf.loc[mask, "speed_limit"].fillna(
                gdf.loc[mask, "road_type"].map(default_speed_limit["urban"])
            )
            mask = (~gdf["urban"]) & gdf["speed_limit"].isna()
            gdf.loc[mask, "speed_limit"] = gdf.loc[mask, "speed_limit"].fillna(
                gdf.loc[mask, "road_type"].map(default_speed_limit["rural"])
            )
        else:
            mask = gdf["speed_limit"].isna()
            gdf.loc[mask, "speed_limit"] = gdf.loc[mask, "speed_limit"].fillna(
                gdf.loc[mask, "road_type"].map(default_speed_limit)
            )
    assert not gdf["speed_limit"].isna().any(), "Some edges have unknown speed limit"
    gdf["speed_limit"] = gdf["speed_limit"].astype(np.float64)
    # Set default number of lanes.
    if "lanes" not in gdf.columns:
        gdf["lanes"] = np.nan
    gdf["default_lanes"] = gdf["lanes"].isna()
    if isinstance(default_nb_lanes, float):
        gdf["lanes"] = gdf["lanes"].fillna(default_nb_lanes)
    elif isinstance(default_nb_lanes, dict):
        if "urban" in default_nb_lanes.keys() and "rural" in default_nb_lanes.keys():
            mask = gdf["urban"] & gdf["lanes"].isna()
            gdf.loc[mask, "lanes"] = gdf.loc[mask, "lanes"].fillna(
                gdf.loc[mask, "road_type"].map(default_nb_lanes["urban"])
            )
            mask = (~gdf["urban"]) & gdf["lanes"].isna()
            gdf.loc[mask, "lanes"] = gdf.loc[mask, "lanes"].fillna(
                gdf.loc[mask, "road_type"].map(default_nb_lanes["rural"])
            )
        else:
            mask = gdf["lanes"].isna()
            gdf.loc[mask, "lanes"] = gdf.loc[mask, "lanes"].fillna(
                gdf.loc[mask, "road_type"].map(default_nb_lanes)
            )
    assert not gdf["lanes"].isna().any(), "Some edges have unknown number of lanes"
    gdf["lanes"] = gdf["lanes"].astype(np.float64)
    return gdf


def remove_duplicates(gdf):
    """Remove the duplicates edges, keeping in order of priority the one in the main graph, with the
    largest capacity and with smallest free-flow travel time."""
    print("Removing duplicate edges")
    n0 = len(gdf)
    l0 = gdf["length"].sum()
    # Sort the dataframe.
    gdf["tt"] = gdf["length"] / (gdf["speed_limit"] / 3.6)
    gdf.sort_values(["tt"], ascending=[True], inplace=True)
    gdf.drop(columns="tt", inplace=True)
    # Drop duplicates.
    gdf.drop_duplicates(subset=["source", "target"], inplace=True)
    n1 = len(gdf)
    if n0 > n1:
        l1 = gdf["length"].sum()
        print("Number of edges removed: {} ({:.2%})".format(n0 - n1, (n0 - n1) / n0))
        print("Edge length removed (m): {:.0f} ({:.2%})".format(l0 - l1, (l0 - l1) / l0))
    return gdf


def select_connected(gdf):
    print("Building graph...")
    G = nx.DiGraph()
    G.add_edges_from(
        map(
            lambda v: (v[0], v[1]),
            gdf[["source", "target"]].values,
        )
    )
    # Keep only the nodes of the largest strongly connected component.
    nodes = max(nx.strongly_connected_components(G), key=len)
    if len(nodes) < G.number_of_nodes():
        print(
            "Warning: discarding {} nodes disconnected from the largest graph component".format(
                G.number_of_nodes() - len(nodes)
            )
        )
        n0 = len(gdf)
        l0 = gdf["length"].sum()
        gdf = gdf.loc[gdf["source"].isin(nodes) & gdf["target"].isin(nodes)].copy()
        n1 = len(gdf)
        l1 = gdf["length"].sum()
        print("Number of edges removed: {} ({:.2%})".format(n0 - n1, (n0 - n1) / n0))
        print("Edge length removed (m): {:.0f} ({:.2%})".format(l0 - l1, (l0 - l1) / l0))
    return gdf


def reindex(gdf):
    gdf["edge_id"] = np.arange(len(gdf), dtype=np.uint64)
    return gdf


def check(gdf, min_nb_lanes: float, min_speed_limit: float, min_length: float):
    gdf["lanes"] = gdf["lanes"].clip(min_nb_lanes)
    gdf["speed_limit"] = gdf["speed_limit"].clip(min_speed_limit)
    gdf["length"] = gdf["length"].clip(min_length)
    # Count number of incoming / outgoing edges for the source / target node.
    target_counts = gdf["target"].value_counts()
    source_counts = gdf["source"].value_counts()
    gdf = gdf.merge(
        target_counts.rename("target_in_degree"), left_on="target", right_index=True, how="left"
    )
    gdf = gdf.merge(
        target_counts.rename("source_in_degree"), left_on="source", right_index=True, how="left"
    )
    gdf = gdf.merge(
        source_counts.rename("target_out_degree"), left_on="target", right_index=True, how="left"
    )
    gdf = gdf.merge(
        source_counts.rename("source_out_degree"), left_on="source", right_index=True, how="left"
    )
    for col in ("target_in_degree", "source_in_degree", "target_out_degree", "source_out_degree"):
        gdf[col] = gdf[col].fillna(0.0).astype(np.uint8)
    # Add oneway column.
    gdf = gdf.merge(
        gdf[["source", "target"]],
        left_on=["source", "target"],
        right_on=["target", "source"],
        how="left",
        indicator="oneway",
        suffixes=("", "_y"),
    )
    gdf.drop(columns=["source_y", "target_y"], inplace=True)
    gdf.drop_duplicates(subset=["edge_id", "source", "target"], inplace=True)
    gdf["oneway"] = (
        gdf["oneway"]
        .cat.remove_unused_categories()
        .cat.rename_categories({"both": False, "left_only": True})
        .astype(bool)
    )
    return gdf


def print_stats(gdf: gpd.GeoDataFrame):
    print("Printing stats")
    nb_nodes = len(set(gdf["source"]).union(set(gdf["target"])))
    print(f"Number of nodes: {nb_nodes:,}")
    nb_edges = len(gdf)
    print(f"Number of edges: {nb_edges:,}")
    if "urban" in gdf.columns:
        nb_urbans = gdf["urban"].sum()
        print(f"Number of urban edges: {nb_urbans:,} ({nb_urbans / nb_edges:.1%})")
        nb_rurals = nb_edges - nb_urbans
        print(f"Number of rural edges: {nb_rurals:,} ({nb_rurals / nb_edges:.1%})")
    nb_roundabouts = gdf["roundabout"].sum()
    print(f"Number of roundabout edges: {nb_roundabouts:,} ({nb_roundabouts / nb_edges:.1%})")
    nb_traffic_signals = gdf["traffic_signals"].sum()
    print(
        f"Number of edges with traffic signals: {nb_traffic_signals:,} ({nb_traffic_signals / nb_edges:.1%})"
    )
    nb_stop_signs = gdf["stop_sign"].sum()
    print(f"Number of edges with stop sign: {nb_stop_signs:,} ({nb_stop_signs / nb_edges:.1%})")
    nb_give_way_signs = gdf["give_way_sign"].sum()
    print(
        f"Number of edges with give_way sign: {nb_give_way_signs:,} ({nb_give_way_signs / nb_edges:.1%})"
    )
    nb_tolls = gdf["toll"].sum()
    print(f"Number of edges with toll: {nb_tolls:,} ({nb_tolls / nb_edges:.1%})")
    tot_length = gdf["length"].sum() / 1e3
    print(f"Total edge length (km): {tot_length:,.3f}")
    if "urban" in gdf.columns:
        urban_length = gdf.loc[gdf["urban"], "length"].sum() / 1e3
        print(
            f"Total urban edge length (km): {urban_length:,.3f} ({urban_length / tot_length:.1%})"
        )
        rural_length = tot_length - urban_length
        print(
            f"Total rural edge length (km): {rural_length:,.3f} ({rural_length / tot_length:.1%})"
        )
