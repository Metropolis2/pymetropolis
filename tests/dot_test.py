import shutil
import tempfile
from pathlib import Path

import pytest

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_pipeline import Config, MetroFile, Step
from pymetropolis.metro_pipeline.dot import (
    RENDER_EXTENSIONS,
    SOURCE_EXTENSIONS,
    STATUS_STYLES,
    build_dot,
    render_dot,
)
from pymetropolis.metro_pipeline.file import MetroTxtFile
from pymetropolis.metro_pipeline.pipeline import MetroPipeline
from pymetropolis.metro_pipeline.types import PathType

HAS_GRAPHVIZ = shutil.which("dot") is not None


class File1(MetroFile):
    path = "file1"


class File2(MetroFile):
    path = "file2"


class File3(MetroFile):
    path = "file3"


class A(Step):
    output_files = {"1": File1}


class B(Step):
    input_files = {"1": File1}
    output_files = {"2": File2}


class C(Step):
    # Reads both File1 (directly from A) and File2 (from B): the A -> C edge is redundant, since
    # C is already reachable from A through B.
    input_files = {"1": File1, "2": File2}
    output_files = {"3": File3}


class NonPrimary(Step):
    priority = 0
    output_files = {"1": File1}


def _build_dot(step_classes, **kwargs) -> str:
    """Builds the DOT source of a pipeline made of `step_classes`, in a throwaway directory."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config({"main_directory": tmp_dir})
        pipeline = MetroPipeline(config, step_classes)
        sequence = pipeline.find_sequence()
        return build_dot(
            [(step, status.name) for step, status in sequence],
            pipeline.steps,
            pipeline.generated_files,
            **kwargs,
        )


def test_every_step_of_the_sequence_is_a_node():
    source = _build_dot([A, B, C])
    for name in ("A", "B", "C"):
        assert f'"{name}" [' in source


def test_dependencies_are_edges():
    source = _build_dot([A, B, C])
    assert '"A" -> "B"' in source
    assert '"B" -> "C"' in source


def test_transitive_reduction_drops_redundant_edge():
    """A -> C is implied by A -> B -> C, so it is dropped (but only with the reduction on)."""
    reduced = _build_dot([A, B, C])
    assert '"A" -> "C"' not in reduced

    full = _build_dot([A, B, C], transitive_reduction=False)
    assert '"A" -> "C"' in full
    assert '"A" -> "B"' in full
    assert '"B" -> "C"' in full


def test_steps_are_coloured_by_status():
    """Steps that never ran are OUTDATED, so they all carry the outdated colours."""
    source = _build_dot([A, B, C])
    style = STATUS_STYLES["OUTDATED"]
    for line in source.splitlines():
        if line.strip().startswith('"A" ['):
            assert f'color="{style.border}"' in line
            assert f'fillcolor="{style.fill}"' in line
            break
    else:
        pytest.fail("no `A` node found")


def test_non_primary_step_is_dashed():
    source = _build_dot([NonPrimary, B])
    for line in source.splitlines():
        if line.strip().startswith('"NonPrimary" ['):
            assert "dashed" in line
            break
    else:
        pytest.fail("no `NonPrimary` node found")


def test_legend_can_be_disabled():
    assert "cluster_legend" in _build_dot([A, B, C])
    assert "cluster_legend" not in _build_dot([A, B, C], legend=False)


def test_dot_output_is_written_verbatim_without_graphviz(monkeypatch):
    """A `.dot` path needs no Graphviz at all, so `dot` is never looked up."""

    def fail(_name):
        pytest.fail("shutil.which should not be called for a .dot output")

    monkeypatch.setattr("pymetropolis.metro_pipeline.dot.shutil.which", fail)
    source = _build_dot([A, B, C])
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "sub" / "graph.dot"
        render_dot(source, path)
        assert path.read_text() == source


def test_missing_graphviz_raises(monkeypatch):
    monkeypatch.setattr("pymetropolis.metro_pipeline.dot.shutil.which", lambda _name: None)
    with tempfile.TemporaryDirectory() as tmp_dir:
        with pytest.raises(MetropyError, match="Graphviz"):
            render_dot("digraph {}", Path(tmp_dir) / "graph.png")


def test_invalid_extension_is_rejected():
    """The CLI validates the output extension with the shared PathType validator."""
    validator = PathType(extensions=RENDER_EXTENSIONS + SOURCE_EXTENSIONS)
    validator.validate(Path("graph.png"))
    validator.validate(Path("graph.dot"))
    with pytest.raises(MetropyError, match="allowed extensions"):
        validator.validate(Path("graph.txt"))


@pytest.mark.skipif(not HAS_GRAPHVIZ, reason="Graphviz `dot` executable is not installed")
def test_render_png():
    source = _build_dot([A, B, C])
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "sub" / "graph.png"
        render_dot(source, path)
        assert path.is_file()
        assert path.read_bytes().startswith(b"\x89PNG")


@pytest.mark.skipif(not HAS_GRAPHVIZ, reason="Graphviz `dot` executable is not installed")
def test_render_svg():
    source = _build_dot([A, B, C])
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "graph.svg"
        render_dot(source, path)
        assert "<svg" in path.read_text()


@pytest.mark.skipif(not HAS_GRAPHVIZ, reason="Graphviz `dot` executable is not installed")
def test_pipeline_run_writes_the_graph():
    """`run(graph_path=...)` saves the graph in dry-run mode, without running anything."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config({"main_directory": tmp_dir})
        pipeline = MetroPipeline(config, [A, B, C])
        path = Path(tmp_dir) / "graph.png"
        pipeline.run(dry_run=True, graph_path=path)
        assert path.is_file()


class TxtFile1(MetroTxtFile):
    path = "txt1"


# Records whether the graph file already existed when the step ran.
GRAPH_SEEN: list[bool] = []


class RunnableStep(Step):
    output_files = {"1": TxtFile1}
    graph_path: Path

    def run(self):
        GRAPH_SEEN.append(self.graph_path.is_file())
        self.output["1"].write("done")


@pytest.mark.skipif(not HAS_GRAPHVIZ, reason="Graphviz `dot` executable is not installed")
def test_graph_is_written_before_the_steps_run():
    """Without --dry-run the graph is saved first, then the pipeline runs."""
    GRAPH_SEEN.clear()
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "graph.png"
        RunnableStep.graph_path = path
        config = Config({"main_directory": tmp_dir})
        pipeline = MetroPipeline(config, [RunnableStep])
        pipeline.run(graph_path=path)
        assert GRAPH_SEEN == [True]
        assert (Path(tmp_dir) / "txt1").read_text() == "done"


def test_every_render_extension_is_a_distinct_known_format():
    assert not set(RENDER_EXTENSIONS) & set(SOURCE_EXTENSIONS)
    assert all(ext.startswith(".") for ext in RENDER_EXTENSIONS + SOURCE_EXTENSIONS)
