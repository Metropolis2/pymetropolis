from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_common import MetropyError

if TYPE_CHECKING:
    import polars as pl


def sum_edge_values_along_path(path: pl.Expr, edge_ids: pl.Series, values: pl.Series) -> pl.Expr:
    import polars as pl

    return path.list.eval(pl.element().replace_strict(edge_ids, values)).list.sum()


def check_free_flow_tt(columns: list[str]):
    if "target_free_flow_tt" not in columns:
        raise MetropyError(
            "Column `target_free_flow_tt` is required in `TargetRoutesFile` but is not defined."
        )
