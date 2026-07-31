from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl


def compute_all_pairs_dijkstra(edges: pl.DataFrame) -> pl.DataFrame:
    import networkx as nx
    import polars as pl

    dtype = edges["source"].dtype
    G = nx.DiGraph()
    G.add_weighted_edges_from(edges.iter_rows(), weight="weight")
    ods = list()
    for origin, data in nx.all_pairs_dijkstra_path_length(G, weight="weight"):
        for destination, weight in data.items():
            ods.append((origin, destination, weight))
    df = pl.DataFrame(
        ods,
        orient="row",
        schema={"origin_id": dtype, "destination_id": dtype, "weight": pl.Float64},
    )
    return df
