import shutil
import tempfile
from pathlib import Path

import polars as pl
import pytest

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.plots import (
    PlotStep,
    escape_tex,
    length_to_inches,
    parse_colors,
    parse_length,
    parse_mode_colors,
    plot_feature_importance,
    plot_mode_share_comparison,
    plot_rc_params,
)
from pymetropolis.metro_pipeline import Config, PopulationStep
from pymetropolis.metro_pipeline.file import MetroPlotFile, PopulationFile
from pymetropolis.metro_pipeline.pipeline import MetroPipeline, StepStatus


class APlotFile(MetroPlotFile):
    path = "plots/a.{format}"


class APopulationPlotFile(MetroPlotFile, PopulationFile):
    path = "plots/{population}/a.{format}"


class APlotStep(PlotStep):
    output_files = {"plot": APlotFile}
    # Size (in inches) of the last figure created by `run`.
    figure_size: tuple[float, float] | None = None

    def run(self):
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots()
        ax.plot([0, 1], [0, 1])
        ax.set_xlabel(self.text("share_of_car (%)"))
        APlotStep.figure_size = tuple(fig.get_size_inches())
        self.output["plot"].write(fig)


class APopulationPlotStep(PlotStep, PopulationStep):
    output_files = {"plot": APopulationPlotFile}

    def run(self):
        import matplotlib.pyplot as plt

        fig, _ = plt.subplots()
        self.output["plot"].write(fig)


def _statuses_by_name(sequence: list) -> dict[str, StepStatus]:
    return {step.__class__.__name__: status for step, status in sequence}


@pytest.mark.parametrize(
    "value,expected",
    [
        ("16cm", (16.0, "cm")),
        (" 6.3 in ", (6.3, "in")),
        ("455.24pt", (455.24, "pt")),
        ("1200px", (1200.0, "px")),
        (".5", (0.5, "in")),
        (6, (6.0, "in")),
        (6.5, (6.5, "in")),
    ],
)
def test_parse_length(value, expected):
    assert parse_length(value) == expected


@pytest.mark.parametrize("value", ["16mm", "cm", "-3cm", "0", 0, True, "16 cm cm", None])
def test_parse_length_rejects_invalid_values(value):
    with pytest.raises(MetropyError):
        parse_length(value)


def test_length_to_inches():
    assert length_to_inches((2.54, "cm"), 300) == pytest.approx(1.0)
    assert length_to_inches((72.27, "pt"), 300) == pytest.approx(1.0)
    assert length_to_inches((3.0, "in"), 300) == 3.0
    assert length_to_inches((1200.0, "px"), 300) == 4.0


def test_escape_tex():
    assert escape_tex("car_driver: 45% (€)") == r"car\_driver: 45\% (\texteuro{})"
    assert escape_tex("3μs") == r"3$\mu$s"
    assert escape_tex("plain text") == "plain text"


def test_plot_rc_params_leaves_out_unset_options():
    assert plot_rc_params() == {}
    rc = plot_rc_params(latex=True, font="serif", fontsize=8, width=4.0, dpi=150, fmt="pdf")
    assert rc["text.usetex"] is True
    assert rc["font.family"] == "serif"
    assert rc["font.size"] == 8
    assert rc["figure.figsize"] == (4.0, 3.0)
    assert rc["savefig.dpi"] == 150
    assert rc["savefig.format"] == "pdf"


def test_plot_rc_params_colors():
    rc = plot_rc_params(colors=["#ff0000", "#00ff00"])
    assert rc["axes.prop_cycle"].by_key() == {"color": ["#ff0000", "#00ff00"]}


def test_parse_colors():
    assert parse_colors(["#FF0000", "blue"]) == ["#ff0000", "#0000ff"]
    # Qualitative colormap: all its colors.
    assert len(parse_colors("Set2")) == 8
    assert len(parse_colors("tab20")) == 20
    # Continuous colormap: sampled colors.
    assert len(parse_colors("viridis")) == 8


@pytest.mark.parametrize("value", ["not_a_cmap", [], ["notacolor"], [1], 3, None])
def test_parse_colors_invalid(value):
    with pytest.raises(MetropyError):
        parse_colors(value)


def test_for_format_resolves_the_placeholder():
    pdf_file = APlotFile.for_format("pdf")
    assert pdf_file.path == "plots/a.pdf"
    assert pdf_file.__name__ == "APlotFile"
    # Memoized: the same format always gives the same class (files compare by class identity).
    assert APlotFile.for_format("pdf") is pdf_file
    assert APlotFile.for_format("svg") is not pdf_file


def test_for_format_composes_with_for_population():
    pop_file = APopulationPlotFile.for_population("trucks")
    pdf_file = pop_file.for_format("pdf")  # ty: ignore[unresolved-attribute]
    assert pdf_file.path == "plots/trucks/a.pdf"
    # In the other order too.
    pdf_file = APopulationPlotFile.for_format("pdf")
    assert pdf_file.for_population("trucks").path == "plots/trucks/a.pdf"  # ty: ignore[unresolved-attribute]


def test_for_format_requires_the_placeholder():
    class NoPlaceholderPlotFile(MetroPlotFile):
        path = "plots/a.png"

    with pytest.raises(MetropyError):
        NoPlaceholderPlotFile.for_format("pdf")


def test_from_dir_rejects_unresolved_placeholders():
    with pytest.raises(MetropyError):
        APlotFile.from_dir(Path("/a"))
    with pytest.raises(MetropyError):
        APopulationPlotFile.for_format("pdf").from_dir(Path("/a"))


def test_for_format_survives_rebase():
    """Rebasing a file on another `main_directory` (as done for a Step reused from a parent
    config) must keep the configured extension."""
    f = APlotFile.for_format("pdf").from_dir(Path("/a"))
    rebased = type(f).from_dir(Path("/b"))
    assert rebased.get_path() == Path("/b/plots/a.pdf")


def test_plot_step_applies_the_configured_style():
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config(
            {"main_directory": tmp_dir, "plots": {"width": "10cm", "format": "pdf", "dpi": 100}}
        )
        pipeline = MetroPipeline(config, [APlotStep])
        pipeline.run_sequence(pipeline.find_sequence())
        assert (Path(tmp_dir) / "plots" / "a.pdf").is_file()
        assert not (Path(tmp_dir) / "plots" / "a.png").exists()
        assert APlotStep.figure_size is not None
        assert APlotStep.figure_size[0] == pytest.approx(10 / 2.54)


def test_plot_step_default_style():
    with tempfile.TemporaryDirectory() as tmp_dir:
        pipeline = MetroPipeline(Config({"main_directory": tmp_dir}), [APlotStep])
        pipeline.run_sequence(pipeline.find_sequence())
        assert (Path(tmp_dir) / "plots" / "a.png").is_file()


def test_plot_step_width_in_pixels():
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config({"main_directory": tmp_dir, "plots": {"width": "600px", "dpi": 100}})
        pipeline = MetroPipeline(config, [APlotStep])
        pipeline.run_sequence(pipeline.find_sequence())
        from PIL import Image

        with Image.open(Path(tmp_dir) / "plots" / "a.png") as img:
            assert img.width == 600


def test_population_plot_step_uses_the_configured_format():
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config({"main_directory": tmp_dir, "plots": {"format": "svg"}})
        pipeline = MetroPipeline(config, [APopulationPlotStep])
        pipeline.run_sequence(pipeline.find_sequence())
        assert (Path(tmp_dir) / "plots" / "population" / "a.svg").is_file()


def test_changing_the_style_reruns_the_plot_step():
    with tempfile.TemporaryDirectory() as tmp_dir:
        config_dict = {"main_directory": tmp_dir, "plots": {"fontsize": 10}}
        pipeline = MetroPipeline(Config(config_dict), [APlotStep])
        pipeline.run_sequence(pipeline.find_sequence())

        pipeline = MetroPipeline(Config(config_dict), [APlotStep])
        assert _statuses_by_name(pipeline.find_sequence()) == {"APlotStep": StepStatus.UP_TO_DATE}

        config_dict["plots"] = {"fontsize": 12}
        pipeline = MetroPipeline(Config(config_dict), [APlotStep])
        assert _statuses_by_name(pipeline.find_sequence()) == {"APlotStep": StepStatus.OUTDATED}


@pytest.mark.skipif(
    shutil.which("latex") is None or shutil.which("dvipng") is None, reason="LaTeX not installed"
)
def test_plots_with_latex():
    """The plots must render with LaTeX, even with special characters in their text."""
    import matplotlib as mpl

    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config(
            {"main_directory": tmp_dir, "plots": {"latex": True, "font": "serif", "dpi": 50}}
        )
        pipeline = MetroPipeline(config, [APlotStep])
        pipeline.run_sequence(pipeline.find_sequence())
        assert (Path(tmp_dir) / "plots" / "a.png").is_file()

        with mpl.rc_context(plot_rc_params(latex=True, dpi=50)):
            fig = plot_mode_share_comparison(
                {"car_driver": 0.6, "walk": 0.4}, {"car_driver": 0.5, "walk": 0.5}, "Share (%)"
            )
            fig.savefig(Path(tmp_dir) / "shares.png")
            df = pl.DataFrame(
                {"feature": ["a_b", "c"], "importance": [0.2, 0.1], "importance_std": [0.0, 0.0]}
            )
            fig = plot_feature_importance(df, "Increase in score (€)")
            fig.savefig(Path(tmp_dir) / "importance.png")


def test_parse_mode_colors():
    assert parse_mode_colors({"car": "#FF0000", "bus": "blue"}) == {
        "car": "#ff0000",
        "bus": "#0000ff",
    }


@pytest.mark.parametrize("value", ["red", ["red"], {"car": "notacolor"}, {"car": 1}])
def test_parse_mode_colors_invalid(value):
    with pytest.raises(MetropyError):
        parse_mode_colors(value)


def _mode_color_map(config_dict: dict, modes: list[str]) -> list[str]:
    import matplotlib as mpl

    with tempfile.TemporaryDirectory() as tmp_dir:
        step = APlotStep(Config({"main_directory": tmp_dir, **config_dict}))
        with mpl.rc_context(step.rc_params()):
            return step.mode_color_map(modes)


def test_mode_color_map():
    plots = {"colors": ["#111111", "#222222"]}
    # No mode colors: the cycle, wrapping around.
    assert _mode_color_map({"plots": plots}, ["a", "b", "c"]) == ["#111111", "#222222", "#111111"]
    # Modes with a color do not consume the cycle.
    plots["mode_colors"] = {"b": "#ff0000"}
    assert _mode_color_map({"plots": plots}, ["a", "b", "c"]) == ["#111111", "#ff0000", "#222222"]
    # Default cycle (tab10).
    assert _mode_color_map({}, ["a"]) == ["#1f77b4"]
