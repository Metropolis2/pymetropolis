from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, override

from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_pipeline.file import MetroFile, MetroPlotFile
from pymetropolis.metro_pipeline.parameters import (
    BoolParameter,
    CustomParameter,
    EnumParameter,
    FloatParameter,
    IntParameter,
    StringParameter,
)
from pymetropolis.metro_pipeline.steps import Step

from .utils import seconds_to_duration_string

if TYPE_CHECKING:
    import matplotlib.pyplot as plt
    import numpy as np
    import polars as pl
    from matplotlib.ticker import FuncFormatter
    from matplotlib.typing import RcKeyType


# Formats in which the plots can be saved.
PLOT_FORMATS = ["png", "pdf", "svg", "pgf"]

# Conversion factors to inches of the units allowed for `plots.width` (except pixels, which depend
# on the resolution). The point is the TeX point (the unit of `\showthe\textwidth`), not the
# PostScript point.
INCHES_PER_UNIT = {"in": 1.0, "cm": 1.0 / 2.54, "pt": 1.0 / 72.27}

LENGTH_REGEX = re.compile(r"\s*(\d+(?:\.\d*)?|\.\d+)\s*(in|cm|pt|px)?\s*")

# Characters which have a special meaning in LaTeX, with their escaped version.
TEX_ESCAPES = {
    "\\": r"\textbackslash{}",
    "%": r"\%",
    "_": r"\_",
    "&": r"\&",
    "#": r"\#",
    "$": r"\$",
    "{": r"\{",
    "}": r"\}",
    "~": r"\textasciitilde{}",
    "^": r"\textasciicircum{}",
    "€": r"\texteuro{}",
    # Used by `seconds_to_duration_string` for microseconds.
    "μ": r"$\mu$",
}


def parse_length(value: Any) -> tuple[float, str]:
    """Parses a length given as a number (in inches) or as a string `"<number><unit>"`, with unit
    `in`, `cm`, `pt` or `px`, and returns it as a tuple `(number, unit)`."""
    if isinstance(value, bool):
        raise MetropyError(f"Invalid length: `{value}`")
    if isinstance(value, int | float):
        length = (float(value), "in")
    elif isinstance(value, str) and (m := LENGTH_REGEX.fullmatch(value)):
        length = (float(m[1]), m[2] or "in")
    else:
        raise MetropyError(
            f"Invalid length: `{value}` (expected a number followed by `in`, `cm`, `pt` or `px`)"
        )
    if length[0] <= 0.0:
        raise MetropyError(f"Length must be positive: `{value}`")
    return length


# Number of colors sampled from a continuous colormap, and maximum size of a colormap which is
# considered qualitative (all its colors are used).
N_SAMPLED_COLORS = 8
MAX_QUALITATIVE_COLORS = 20


def parse_colors(value: Any) -> list[str]:
    """Parses a colormap name or a list of colors and returns the colors as HEX strings.

    A qualitative colormap (e.g., `Set2`) gives all its colors; a continuous colormap (e.g.,
    `viridis`) gives colors sampled evenly from it.
    """
    import matplotlib as mpl
    from matplotlib.colors import ListedColormap, is_color_like, to_hex

    if isinstance(value, str):
        if value not in mpl.colormaps:
            raise MetropyError(f"Unknown colormap: `{value}`")
        cmap = mpl.colormaps[value]
        if isinstance(cmap, ListedColormap) and cmap.N <= MAX_QUALITATIVE_COLORS:
            return [to_hex(cmap(i)) for i in range(cmap.N)]
        return [
            to_hex(cmap(x)) for x in [i / (N_SAMPLED_COLORS - 1) for i in range(N_SAMPLED_COLORS)]
        ]
    if isinstance(value, list) and value:
        for c in value:
            if not isinstance(c, str) or not is_color_like(c):
                raise MetropyError(f"Invalid color: `{c}`")
        return [to_hex(c) for c in value]
    raise MetropyError(f"Invalid colors: `{value}` (expected a colormap name or a list of colors)")


def length_to_inches(length: tuple[float, str], dpi: float) -> float:
    """Converts a length returned by `parse_length` to inches, with pixels converted according to
    the resolution `dpi`."""
    value, unit = length
    if unit == "px":
        return value / dpi
    return value * INCHES_PER_UNIT[unit]


def escape_tex(text: str) -> str:
    """Escapes the LaTeX special characters of `text`."""
    return "".join(TEX_ESCAPES.get(c, c) for c in text)


def tex(text: str) -> str:
    """Returns `text`, escaped if the text of the plots is currently rendered with LaTeX.

    Every string drawn on a plot which is not known to be free of LaTeX special characters (e.g.,
    mode names with `_`, percentages, `€`) must go through this function, otherwise LaTeX fails to
    render it.
    """
    import matplotlib as mpl

    if mpl.rcParams["text.usetex"]:
        return escape_tex(text)
    return text


def percent_label(x: float) -> str:
    """Formats `x` as a percentage, to label bars (`ax.bar_label(bars, fmt=percent_label)`)."""
    return tex(f"{x:.0%}")


def duration_formatter() -> FuncFormatter:
    """Returns a tick formatter which shows the values (in seconds) as durations."""
    from matplotlib.ticker import FuncFormatter

    return FuncFormatter(lambda x, pos: tex(seconds_to_duration_string(x)))


def figure_size(aspect: float | None = None, height: float | None = None) -> tuple[float, float]:
    """Returns the size (in inches) of a figure with the configured width (see `PlotStep`).

    The height is `height` (in inches) if given, otherwise `aspect` times the width if given,
    otherwise the configured default height.
    """
    import matplotlib as mpl

    width, default_height = mpl.rcParams["figure.figsize"]
    if height is not None:
        return (width, height)
    if aspect is not None:
        return (width, width * aspect)
    return (width, default_height)


def plot_rc_params(
    *,
    latex: bool = False,
    font: str | None = None,
    fontsize: float | None = None,
    width: float | None = None,
    dpi: int | None = None,
    fmt: str | None = None,
    colors: list[str] | None = None,
    aspect: float = 3 / 4,
) -> dict[RcKeyType, Any]:
    """Returns the matplotlib rcParams which apply the given plot style.

    `width` is in inches; the default height of the figures is `aspect` times the width. The
    options which are not set are left out, so that matplotlib's defaults apply.
    """
    rc: dict[RcKeyType, Any] = {}
    if latex:
        rc["text.usetex"] = True
        # With the PGF format, the fonts are set by the LaTeX document the figure is included in.
        rc["pgf.rcfonts"] = False
    if font is not None:
        rc["font.family"] = font
    if fontsize is not None:
        # The other font sizes (labels, ticks, legend, ...) are relative to this one.
        rc["font.size"] = fontsize
    if width is not None:
        rc["figure.figsize"] = (width, width * aspect)
    if dpi is not None:
        rc["savefig.dpi"] = dpi
    if fmt is not None:
        rc["savefig.format"] = fmt
    if colors is not None:
        from cycler import cycler

        rc["axes.prop_cycle"] = cycler(color=colors)
    return rc


class PlotStep(Step):
    """An abstract Step for Steps generating plots.

    The plot style (fonts, width, format, ...) is set globally by the `plots.*` parameters and
    applied through matplotlib's rcParams while the Step runs, so the plotting code itself is plain
    matplotlib. Strings drawn on the plots must go through `tex` (or `self.text`), and figures
    should be created with the default size or with `figure_size` to keep the configured width.

    The `{format}` placeholder in the path of the `MetroPlotFile` outputs of the Step is
    resolved from `plots.format`.
    """

    latex = BoolParameter(
        "plots.latex",
        default=False,
        description="Whether the text of the plots is rendered with LaTeX.",
        note=(
            "This requires a working LaTeX installation (and `dvipng` for the PNG format). Use it "
            'with `font = "serif"` to get the default LaTeX font (Computer Modern).'
        ),
        shared=True,
    )
    font = StringParameter(
        "plots.font",
        description="Font family used in the plots.",
        example='`"serif"`, `"sans-serif"`, `"DejaVu Sans"`',
        note=(
            "Either a generic family (`serif`, `sans-serif`, `monospace`, ...) or the name of a "
            "font installed on the system. When `plots.latex` is true, only generic families are "
            "supported. Default is matplotlib's default font."
        ),
        shared=True,
    )
    fontsize = FloatParameter(
        "plots.fontsize",
        lower_bound=1.0,
        description="Base font size of the plots, in points.",
        note=(
            "The size of the labels, ticks, legend, etc. is set relative to this size. "
            "Default is matplotlib's default (10)."
        ),
        shared=True,
    )
    width = CustomParameter(
        "plots.width",
        validator=parse_length,
        validator_description=(
            "a positive number (in inches) or a string `<number><unit>` with unit `in`, `cm`, "
            "`pt` or `px`"
        ),
        description="Width of the plots.",
        example='`"16cm"`, `"6.3in"`, `"455.24pt"`, `"1200px"`',
        note=(
            "To match the width of a LaTeX document, use the value in points printed by "
            '`\\showthe\\textwidth` (e.g., `"455.24pt"`): the plots then do not need to be '
            "rescaled and their fonts have the configured size in the document. For a "
            'presentation or a web page, a width in pixels (e.g., `"1200px"`) gives an image '
            "with exactly that number of pixels, but note that the font size, in points, is then "
            "relative to `plots.dpi`. The height of the plots is set relative to the width. "
            "Default is matplotlib's default (6.4 inches)."
        ),
        shared=True,
    )
    dpi = IntParameter(
        "plots.dpi",
        default=300,
        lower_bound=1,
        description="Resolution of the plots, in dots per inch.",
        note="Only relevant for the raster formats (PNG) and for a width in pixels.",
        shared=True,
    )
    plot_format = EnumParameter(
        "plots.format",
        values=PLOT_FORMATS,
        default="png",
        description="File format of the plots.",
        note=(
            "The extension of the plot files follows this format. Use `pgf` to include the plots "
            "in a LaTeX document with `\\input`, so that they use the fonts of the document."
        ),
        shared=True,
    )

    colors = CustomParameter(
        "plots.colors",
        validator=parse_colors,
        validator_description="a colormap name or a list of colors",
        description="Colors used, in order, for the series of the plots.",
        example='`"Set2"`, `["#E69F00", "#56B4E9", "#009E73"]`',
        note=(
            "Either the name of a matplotlib colormap or a list of colors (HEX codes or color "
            "names). All the colors of a qualitative colormap are used; for a continuous colormap "
            f"(e.g., `viridis`), {N_SAMPLED_COLORS} colors are sampled evenly. "
            "Default is matplotlib's default (`tab10`)."
        ),
        shared=True,
    )

    @override
    def _resolve_file_class(self, file_class: type[MetroFile], population: str) -> type[MetroFile]:
        file_class = super()._resolve_file_class(file_class, population)
        if issubclass(file_class, MetroPlotFile):
            assert self.plot_format is not None
            file_class = file_class.for_format(self.plot_format)
        return file_class

    def rc_params(self) -> dict[RcKeyType, Any]:
        """Returns the matplotlib rcParams of the configured plot style."""
        assert self.dpi is not None
        width = None if self.width is None else length_to_inches(self.width, self.dpi)
        return plot_rc_params(
            latex=bool(self.latex),
            font=self.font,
            fontsize=self.fontsize,
            width=width,
            dpi=self.dpi,
            fmt=self.plot_format,
            colors=self.colors,
        )

    def text(self, text: str) -> str:
        """Returns `text`, escaped if the plots are rendered with LaTeX."""
        return escape_tex(text) if self.latex else text

    def check_latex(self):
        """Raises an error if LaTeX is enabled but the executables it requires are missing."""
        import shutil

        if not self.latex:
            return
        required = ["latex"]
        if self.plot_format == "png":
            required.append("dvipng")
        for exe in required:
            if shutil.which(exe) is None:
                raise MetropyError(
                    f"`plots.latex` is true but `{exe}` cannot be found: install LaTeX (and "
                    "`dvipng`) or set `plots.latex` to false"
                )

    @override
    def execute(self):
        import matplotlib as mpl

        self.check_latex()
        with mpl.rc_context(self.rc_params()):
            super().execute()


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
        fig.colorbar(hb, ax=ax, label=tex("Number of ODs (log scale)"))
    else:
        ax.scatter(observed, predicted, s=14, alpha=0.6, edgecolors="none")

    ax.plot([0, vmax], [0, vmax], linestyle="--", color="black", linewidth=1, label="y = x")

    annotation = tex(f"N = {n}\nRMSE = {seconds_to_duration_string(rmse)}")
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
    ax.set_xlabel(tex(xlabel))
    ax.set_ylabel(tex(ylabel))
    formatter = duration_formatter()
    ax.xaxis.set_major_formatter(formatter)
    ax.yaxis.set_major_formatter(formatter)
    ax.annotate(annotation, xy=(0.02, 0.98), xycoords="axes fraction", va="top")
    ax.grid()
    ax.legend(loc="lower right")
    fig.tight_layout()
    return fig


def plot_feature_importance(
    df: pl.DataFrame,
    xlabel: str,
    max_features: int | None = None,
    labels: dict[str, str] | None = None,
) -> plt.Figure:
    """Plots the feature importance of `df` (columns `feature`, `importance`, `importance_std`) as
    a horizontal bar chart, with the most important feature on top.

    With `max_features`, only that many features are plotted, the most important ones. The
    features left out of the figure are unaffected otherwise: they are still part of `df`.

    With `labels`, the features are shown with their human-friendly name from that dictionary (the
    features missing from it are shown with their name in `df`).
    """
    import matplotlib as mpl
    import matplotlib.pyplot as plt

    if max_features is not None:
        df = df.sort("importance", descending=True).head(max_features)

    # `barh` draws the first row at the bottom, so the rows are sorted by increasing importance to
    # get the most important feature on top.
    df = df.sort("importance")
    y = list(range(len(df)))

    # The number of features is set by the config (a few dozens in practice), so the height of the
    # figure must grow with it (and with the font size) for the labels to stay readable.
    font_scale = mpl.rcParams["font.size"] / 10.0
    fig, ax = plt.subplots(figsize=figure_size(height=(0.3 * len(df) + 1.5) * font_scale))
    ax.barh(y, df["importance"], xerr=df["importance_std"], capsize=2, zorder=2)
    if labels is None:
        labels = {}
    ax.set_yticks(y, [tex(labels.get(feature, feature)) for feature in df["feature"]])
    # A feature the model does not use has a zero importance (and can be slightly negative, when
    # shuffling it happens to improve the predictions).
    ax.axvline(0.0, color="black", linewidth=1, zorder=3)
    ax.set_xlabel(tex(xlabel))
    ax.grid(which="major", axis="x", zorder=1)
    fig.tight_layout(pad=0.5)
    return fig


def plot_mode_share_comparison(
    survey_shares: dict[str, float], sim_shares: dict[str, float], xlabel: str
) -> plt.Figure:
    """Plots the mode shares of the survey and of the simulation side by side, as a horizontal bar
    chart."""
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    modes = sorted(set(survey_shares) | set(sim_shares))
    survey_values = [survey_shares.get(mode, 0.0) for mode in modes]
    sim_values = [sim_shares.get(mode, 0.0) for mode in modes]
    y = list(range(len(modes)))
    height = 0.35

    fig, ax = plt.subplots()
    survey_bars = ax.barh(
        [i + height / 2 for i in y], survey_values, height=height, label="Survey", zorder=1
    )
    sim_bars = ax.barh(
        [i - height / 2 for i in y], sim_values, height=height, label="Simulation", zorder=1
    )
    ax.bar_label(survey_bars, fmt=percent_label, padding=5, zorder=3)
    ax.bar_label(sim_bars, fmt=percent_label, padding=5, zorder=3)
    ax.set_yticks(y, [tex(mode) for mode in modes])
    ax.xaxis.set_major_formatter(PercentFormatter(xmax=1, decimals=0))
    ax.set_xlim(left=0)
    ax.set_xlabel(tex(xlabel))
    ax.legend()
    ax.grid(which="major", axis="x", zorder=2)
    fig.tight_layout(pad=0.5)
    return fig
