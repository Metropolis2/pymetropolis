"""Rendering of a Step dependency graph as a Graphviz diagram.

`build_dot` is pure: it turns the Steps of a run (and the files linking them) into DOT source.
`render_dot` is the only part that touches the filesystem or shells out to Graphviz.

This module must not import `pipeline.py` at runtime (`pipeline.py` imports it), hence Step
statuses crossing the boundary as plain strings rather than as `StepStatus` members.
"""

from __future__ import annotations

import shutil
import subprocess
from typing import TYPE_CHECKING, NamedTuple

from pymetropolis.metro_common import MetropyError

if TYPE_CHECKING:
    from pathlib import Path

    from .file import MetroFile
    from .steps import Step

# Extensions rendered by Graphviz, and extensions written as DOT source directly (no Graphviz
# needed). `-T` is derived from the extension, so every rendered one must be a valid Graphviz
# output format.
RENDER_EXTENSIONS = [".png", ".svg", ".jpg", ".jpeg", ".pdf", ".gif", ".ps"]
SOURCE_EXTENSIONS = [".dot", ".gv"]

# Graph-level layout attributes. `rankdir="TB"` and the transitive reduction applied in
# `build_dot` were both picked by measuring edge crossings on a large real pipeline (84 steps):
# top-to-bottom beat left-to-right on both crossings and aspect ratio, and `concentrate=true`
# made crossings worse, so it is deliberately not set. `mclimit` raises Graphviz's
# crossing-minimization budget, which is cheap at these graph sizes.
GRAPH_ATTRS = [
    'rankdir="TB"',
    'splines="spline"',
    'bgcolor="white"',
    "nodesep=0.28",
    "ranksep=0.75",
    "mclimit=20",
]


class NodeStyle(NamedTuple):
    border: str
    fill: str
    font: str


# Hex equivalents of the RGB constants used by `MetroPipeline.print_sequence`, so that the graph
# and the terminal output share a legend. Keyed by `StepStatus` member name.
STATUS_STYLES: dict[str, NodeStyle] = {
    "UP_TO_DATE": NodeStyle(border="#787878", fill="#f2f2f2", font="#555555"),
    "INVALIDATED": NodeStyle(border="#e6a000", fill="#fff4de", font="#7a5400"),
    "OUTDATED": NodeStyle(border="#dc2828", fill="#fde8e8", font="#8f1616"),
}
STATUS_LABELS: dict[str, str] = {
    "UP_TO_DATE": "up to date",
    "INVALIDATED": "invalidated",
    "OUTDATED": "outdated",
}
# Border used for a Step whose output is reused from an ancestor config (see `reuse.py`).
REUSE_BORDER = "#64aadc"


def _escape(value: object) -> str:
    """Escapes a value for use inside a double-quoted DOT attribute."""
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def _step_edges(
    sequence_steps: list[Step],
    steps: dict[Step, dict[str, set[MetroFile]]],
    generated_files: dict[MetroFile, set[Step]],
) -> set[tuple[str, str]]:
    """Returns the (producer, consumer) pairs of Steps of `sequence_steps` linked by a MetroFile.

    Both ends must be in `sequence_steps`: a Step that is not part of the run has no node to
    attach an edge to.
    """
    in_sequence = set(sequence_steps)
    edges: set[tuple[str, str]] = set()
    for step in sequence_steps:
        spec = steps[step]
        for f in spec["required_inputs"] | spec["optional_inputs"]:
            for producer in generated_files.get(f, set()):
                if producer is not step and producer in in_sequence:
                    edges.add((str(producer), str(step)))
    return edges


def _transitive_reduction(edges: set[tuple[str, str]]) -> set[tuple[str, str]]:
    """Drops the edges of `edges` that are already implied by a longer path.

    Execution order is untouched: a transitive reduction preserves reachability exactly, it only
    removes the shortcuts that would otherwise clutter the drawing (on a large real pipeline this
    cut 180 edges to 111 and edge crossings by an order of magnitude).
    """
    import networkx as nx

    graph = nx.DiGraph()
    graph.add_edges_from(edges)
    if not nx.is_directed_acyclic_graph(graph):
        # Should not happen: the sequence is built by a topological walk. Keep every edge rather
        # than fail the whole run over a diagram.
        return edges
    return set(nx.transitive_reduction(graph).edges())


def _legend() -> list[str]:
    """Returns the DOT statements for a standalone legend cluster.

    The legend is kept isolated from the Step nodes: clustering the Steps themselves (by package)
    was measured to constrain rank assignment and make crossings worse.
    """
    lines = [
        "  subgraph cluster_legend {",
        '    label="Legend"; fontname="Helvetica"; fontsize=11; color="#cccccc";',
        "    rank=same;",
    ]
    for status, style in STATUS_STYLES.items():
        lines.append(
            f'    "legend_{status}" [label="{STATUS_LABELS[status]}", color="{style.border}", '
            f'fillcolor="{style.fill}", fontcolor="{style.font}"];'
        )
    lines.append(
        f'    "legend_REUSED" [label="reused", color="{REUSE_BORDER}", fillcolor="white", '
        'fontcolor="#555555"];'
    )
    lines.append(
        '    "legend_NON_PRIMARY" [label="non-primary", '
        'style="rounded,filled,dashed", color="#787878", fillcolor="#f2f2f2", '
        'fontcolor="#555555"];'
    )
    lines.append("  }")
    return lines


def build_dot(
    sequence: list[tuple[Step, str]],
    steps: dict[Step, dict[str, set[MetroFile]]],
    generated_files: dict[MetroFile, set[Step]],
    reused: dict[Step, str] | None = None,
    transitive_reduction: bool = True,
    legend: bool = True,
) -> str:
    """Builds the DOT source of the Step dependency graph of one run.

    `sequence` is the pipeline's ordered `(step, status_name)` pairs (`status_name` being a
    `StepStatus` member name), `steps` and `generated_files` are `MetroPipeline`'s own graph
    bookkeeping, and `reused` maps a Step to the name of the ancestor config file its output is
    reused from.
    """
    reused = reused or {}
    sequence_steps = [step for step, _ in sequence]
    edges = _step_edges(sequence_steps, steps, generated_files)
    if transitive_reduction:
        edges = _transitive_reduction(edges)

    lines = ["digraph pipeline {"]
    lines += [f"  {attr};" for attr in GRAPH_ATTRS]
    lines.append(
        '  node [shape="box", style="rounded,filled", fontname="Helvetica", fontsize=10, '
        'margin="0.14,0.07", penwidth=1.2];'
    )
    lines.append('  edge [color="#9aa0a6", arrowsize=0.65, penwidth=0.9];')
    if legend:
        lines += _legend()

    for step, status in sequence:
        style = STATUS_STYLES[status]
        name = _escape(step)
        label = name
        border = style.border
        source_config = reused.get(step)
        if source_config is not None:
            # Mirrors the `(from: <config>)` annotation of `MetroPipeline.print_sequence`.
            label = f"{name}\\n(from: {_escape(source_config)})"
            border = REUSE_BORDER
        attrs = [
            f'label="{label}"',
            f'color="{border}"',
            f'fillcolor="{style.fill}"',
            f'fontcolor="{style.font}"',
        ]
        if not step.is_primary():
            attrs.append('style="rounded,filled,dashed"')
        lines.append(f'  "{name}" [{", ".join(attrs)}];')

    for producer, consumer in sorted(edges):
        lines.append(f'  "{_escape(producer)}" -> "{_escape(consumer)}";')
    lines.append("}")
    return "\n".join(lines) + "\n"


def render_dot(source: str, path: Path) -> None:
    """Writes the DOT `source` to `path`, rendering it with Graphviz unless `path` is DOT source.

    Raises a MetropyError if `path` needs Graphviz and the `dot` executable cannot be found, so
    that an explicitly requested graph never silently goes missing.
    """
    path.parent.mkdir(exist_ok=True, parents=True)
    if path.suffix in SOURCE_EXTENSIONS:
        path.write_text(source)
        return
    exec_path = shutil.which("dot")
    if exec_path is None:
        raise MetropyError(
            f"Graphviz `dot` executable not found in PATH (required to render `{path}`).\n"
            "Install Graphviz (e.g. `apt install graphviz` or `brew install graphviz`), "
            "or use a `.dot` output path to save the graph source without rendering it."
        )
    fmt = path.suffix.removeprefix(".")
    res = subprocess.run(
        [exec_path, f"-T{fmt}", "-o", str(path)], input=source, text=True, check=False
    )
    if res.returncode:
        raise MetropyError(f"Graphviz failed to render the pipeline graph to `{path}`.")
    if not path.exists():
        raise MetropyError(f"Output file not written: `{path}`")
