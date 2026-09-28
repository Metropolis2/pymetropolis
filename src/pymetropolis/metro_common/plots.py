from __future__ import annotations

from typing import TYPE_CHECKING

from .utils import seconds_to_duration_string

if TYPE_CHECKING:
    import matplotlib.pyplot as plt
    import numpy as np
    import polars as pl


def plot_travel_time_comparison(
    observed: np.ndarray,
    predicted: np.ndarray,
    rmse: float,
    xlabel: str,
    ylabel: str,
    show_regression: bool = False,
    outlier_quantile: float | None = None,
) -> plt.Figure:
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.ticker import FuncFormatter

    n = observed.size
    fig, ax = plt.subplots()
    if outlier_quantile is None:
        outlier_quantile = 1.0
    vmax = (
        max(np.quantile(observed, outlier_quantile), np.quantile(predicted, outlier_quantile))
        * 1.02
    )
    mask = (observed <= vmax) & (predicted <= vmax)

    # Overplotting makes a plain scatter unreadable past a few hundred points;
    # switch to a log-scaled 2D density above that so dense datasets stay legible.
    if n > 500:
        hb = ax.hexbin(
            observed[mask], predicted[mask], gridsize=50, mincnt=1, bins="log", cmap="viridis"
        )
        fig.colorbar(hb, ax=ax, label="Number of ODs (log scale)")
    else:
        ax.scatter(observed, predicted, s=14, alpha=0.6, edgecolors="none")

    ax.plot([0, vmax], [0, vmax], linestyle="--", color="black", linewidth=1, label="y = x")

    annotation = f"N = {n}\nRMSE = {seconds_to_duration_string(rmse)}"
    if show_regression and n >= 2:
        b, a = np.polyfit(observed[mask], predicted[mask], 1)
        ax.plot(
            [0, vmax],
            [a, a + b * vmax],
            color="#D55E00",
            linewidth=1.5,
            label=f"y = {a:.1f} + {b:.2f}x",
        )
        # annotation += f"\ny = {a:.1f} + {b:.2f}x"

    ax.set_xlim(0, vmax)
    ax.set_ylim(0, vmax)
    ax.set_aspect("equal", adjustable="box")
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    formatter = FuncFormatter(lambda x, pos: seconds_to_duration_string(x))
    ax.xaxis.set_major_formatter(formatter)
    ax.yaxis.set_major_formatter(formatter)
    ax.annotate(annotation, xy=(0.02, 0.98), xycoords="axes fraction", va="top")
    ax.grid()
    ax.legend(loc="lower right")
    fig.tight_layout()
    return fig


def plot_feature_importance(
    df: pl.DataFrame, xlabel: str, max_features: int | None = None
) -> plt.Figure:
    """Plots the feature importance of `df` (columns `feature`, `importance`, `importance_std`) as
    a horizontal bar chart, with the most important feature on top.

    With `max_features`, only that many features are plotted, the most important ones. The
    features left out of the figure are unaffected otherwise: they are still part of `df`.
    """
    import matplotlib.pyplot as plt

    if max_features is not None:
        df = df.sort("importance", descending=True).head(max_features)

    # `barh` draws the first row at the bottom, so the rows are sorted by increasing importance to
    # get the most important feature on top.
    df = df.sort("importance")
    y = list(range(len(df)))

    # The number of features is set by the config (a few dozens in practice), so the height of the
    # figure must grow with it for the labels to stay readable.
    fig, ax = plt.subplots(figsize=(8.0, 0.3 * len(df) + 1.5))
    ax.barh(y, df["importance"], xerr=df["importance_std"], capsize=2, zorder=2)
    ax.set_yticks(y, df["feature"])
    # A feature the model does not use has a zero importance (and can be slightly negative, when
    # shuffling it happens to improve the predictions).
    ax.axvline(0.0, color="black", linewidth=1, zorder=3)
    ax.set_xlabel(xlabel)
    ax.grid(which="major", axis="x", zorder=1)
    fig.tight_layout(pad=0.5)
    return fig
