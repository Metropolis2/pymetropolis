from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl


def get_largest_strongly_connected_component_nodes(df: pl.DataFrame) -> set:
    import networkx as nx

    assert not df.is_empty(), "No edge given."
    G = nx.DiGraph()
    G.add_edges_from((v[0], v[1]) for v in df.select("source", "target").to_numpy())
    nodes = max(nx.strongly_connected_components(G), key=len)
    return nodes  # ty: ignore[invalid-return-type]
