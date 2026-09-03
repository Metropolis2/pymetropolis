from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_common.utils import seconds_to_duration_string

if TYPE_CHECKING:
    import matplotlib.pyplot as plt
    import numpy as np


def plot_travel_time_comparison(
    observed: np.ndarray, predicted: np.ndarray, rmse: float, xlabel: str, ylabel: str
) -> plt.Figure:
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    n = observed.size
    fig, ax = plt.subplots()
    vmax = max(observed.max(), predicted.max()) * 1.05

    # Overplotting makes a plain scatter unreadable past a few hundred points;
    # switch to a log-scaled 2D density above that so dense datasets stay legible.
    if n > 500:
        hb = ax.hexbin(observed, predicted, gridsize=50, mincnt=1, bins="log", cmap="viridis")
        fig.colorbar(hb, ax=ax, label="Number of ODs (log scale)")
    else:
        ax.scatter(observed, predicted, s=14, alpha=0.6, edgecolors="none")

    ax.plot([0, vmax], [0, vmax], linestyle="--", color="black", linewidth=1, label="y = x")
    ax.set_xlim(0, vmax)
    ax.set_ylim(0, vmax)
    ax.set_aspect("equal", adjustable="box")
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    formatter = FuncFormatter(lambda x, pos: seconds_to_duration_string(x))
    ax.xaxis.set_major_formatter(formatter)
    ax.yaxis.set_major_formatter(formatter)
    ax.annotate(
        f"N = {n}\nRMSE = {seconds_to_duration_string(rmse)}",
        xy=(0.02, 0.98),
        xycoords="axes fraction",
        va="top",
    )
    ax.grid()
    ax.legend(loc="lower right")
    fig.tight_layout()
    return fig
