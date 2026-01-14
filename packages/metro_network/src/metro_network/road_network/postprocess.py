import geopandas as gpd
import networkx as nx
import numpy as np
from metro_pipeline import Config, ConfigTable, ConfigValue, Step

from .files import CLEAN_EDGES_FILE, RAW_EDGES_FILE

EPSILON = np.finfo(float).eps

MIN_NB_LANES = ConfigValue(
    "road_network_postprocess.min_nb_lanes",
    key="min_nb_lanes",
    default=1.0,
    expected_type=float,  # TODO positive float dtype
    description="Minimum number of lanes allowed on edges.",
    example="`0.5`",
)

MIN_SPEED_LIMIT = ConfigValue(
    "road_network_postprocess.min_speed_limit",
    key="min_speed_limit",
    default=EPSILON,
    expected_type=float,
    description="Minimum speed limit allowed on edges (in km/h).",
)

MIN_LENGTH = ConfigValue(
    "road_network_postprocess.min_length",
    key="min_length",
    default=0.0,
    expected_type=float,
    description="Minimum length allowed on edges (in meters).",
)

REMOVE_DUPLICATES = ConfigValue(
    "road_network_postprocess.remove_duplicates",
    key="remove_duplicates",
    default=False,
    expected_type=bool,
    description="Whether the duplicate edges (edges with same source and target) should be removed.",
    note="If `True`, the edge with the smallest travel time is kept.",
)

ENSURE_CONNECTED = ConfigValue(
    "road_network_postprocess.ensure_connected",
    key="ensure_connected",
    default=False,
    expected_type=bool,
    description=(
        "Whether the network should be restricted to the largest strongly connected component of "
        "the underlying graph."
    ),
    note=(
        "If `False`, it is the user's responsibility to ensure that all origin-destination pairs "
        "are feasible."
    ),
)

REINDEX = ConfigValue(
    "road_network_postprocess.reindex",
    key="reindex",
    default=False,
    expected_type=bool,
    description=(
        "If `true`, the edges are re-index after the postprocessing so that they are indexed from "
        "0 to n-1."
    ),
)

DEFAULT_SPEED_LIMIT = ConfigValue(
    "road_network_postprocess.default_speed_limit",
    key="default_speed_limit",
    default=1.0,
    expected_type=float | dict[str, float] | dict[str, dict[str, float]],
    description="Default speed limit (in km/h) to use for edges with no specified value.",
    note=(
        "The value is either a scalar value to be applied to all edges with no specified value, a "
        "table `road_type -> speed_limit` or two tables `road_type -> speed_limit`, for urban and "
        "rural edges."
    ),
)
DEFAULT_NB_LANES = ConfigValue(
    "road_network_postprocess.default_nb_lanes",
    key="default_nb_lanes",
    default=1.0,
    expected_type=float | dict[str, float] | dict[str, dict[str, float]],
    description="Default number of lanes to use for edges with no specified value.",
    note=(
        "The value is either a scalar value to be applied to all edges with no specified value, a "
        "table `road_type -> nb_lanes` or two tables `road_type -> nb_lanes`, for urban and rural "
        "edges."
    ),
)

POSTPROCESS_TABLE = ConfigTable(
    "road_network_postprocess",
    "road_network_postprocess",
    items=[
        MIN_NB_LANES,
        MIN_SPEED_LIMIT,
        MIN_LENGTH,
        REMOVE_DUPLICATES,
        ENSURE_CONNECTED,
        REINDEX,
        DEFAULT_SPEED_LIMIT,
        DEFAULT_NB_LANES,
    ],
    description="Post-process the imported road network to make it compatible with METROPOLIS2",
)

POSTPROCESS_CONFIG = [POSTPROCESS_TABLE] + POSTPROCESS_TABLE.items


def postprocess(config: Config) -> bool:
    """Reads a GeoDataFrame of edges and performs various operations to make the data ready to use
    with METROPOLIS2.
    Saves the results to the given output file.
    """
    gdf = RAW_EDGES_FILE.read(config)
    gdf = set_default_values(gdf, config)
    if config[REMOVE_DUPLICATES]:
        gdf = remove_duplicates(gdf)
    if config[ENSURE_CONNECTED]:
        gdf = select_connected(gdf)
    if config[REINDEX]:
        gdf = reindex(gdf)
    gdf = check(gdf, config)
    gdf.sort_values("edge_id", inplace=True)
    CLEAN_EDGES_FILE.save(gdf, config)
    return True


POSTPROCESS_ROAD_NETWORK = Step(
    "postprocess-road-network",
    postprocess,
    required_files=[RAW_EDGES_FILE],
    output_files=[CLEAN_EDGES_FILE],
    config_values=[
        MIN_NB_LANES,
        MIN_SPEED_LIMIT,
        MIN_LENGTH,
        REMOVE_DUPLICATES,
        ENSURE_CONNECTED,
        REINDEX,
        DEFAULT_SPEED_LIMIT,
        DEFAULT_NB_LANES,
    ],
)


def set_default_values(gdf, config):
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
    default_speed_limit = config[DEFAULT_SPEED_LIMIT]
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
    assert not gdf["speed_limit"].isna().any()
    gdf["speed_limit"] = gdf["speed_limit"].astype(np.float64)
    # Set default number of lanes.
    if "lanes" not in gdf.columns:
        gdf["lanes"] = np.nan
    gdf["default_lanes"] = gdf["lanes"].isna()
    default_nb_lanes = config[DEFAULT_NB_LANES]
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
    assert not gdf["lanes"].isna().any()
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


def check(gdf, config):
    gdf["lanes"] = gdf["lanes"].clip(config[MIN_NB_LANES])
    gdf["speed_limit"] = gdf["speed_limit"].clip(config[MIN_SPEED_LIMIT])
    gdf["length"] = gdf["length"].clip(config[MIN_SPEED_LIMIT])
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
