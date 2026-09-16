"""Decides, for each Step of a config with a `parent_config`, whether an ancestor's own run
already holds a valid, reusable copy of that Step's output.

This module only computes the decision (`compute_source_config`); it does not change what
`MetroPipeline` actually executes or where it reads/writes files. See `CLAUDE.md` for the
rationale (avoiding duplicate work/output across a chain of derived scenario configs).

A Step `s` resolved against config `C` (with parent `P`) is reusable from a source config
`S` (one of `C.chain()`, `S != C`) when, walking from `P` up to `S`:

1. An equivalent Step exists in each config of the chain up to `S` (same class, same
   population) with an identical `config_hash()` — i.e. every `Parameter` the Step reads
   resolves to the same value there as in `C`.
2. That equivalent Step's own recorded run at `S` is still fresh
   (`not equivalent.update_required()`).
3. Every required/optional input file `s` actually reads (per `C`'s own step graph) is produced,
   in that same graph, by a Step whose resolved source is `S` too — `config_hash()` alone only
   covers `s`'s own declared `Parameter`s, not whether its upstream inputs match.

A Step failing any of these is its own source (today's behavior: it is recomputed in `C`'s own
`main_directory`).

Known limitation: a `custom_steps`-overridden Step class is probed against every ancestor using
the *same*, fully-resolved class as the config actually being run, not the class an ancestor
would have used had it been run standalone with its own (shorter) `custom_steps` list. This only
matters when an override changes a Step's `Parameter`s or `run()` behavior, which is rare enough
to leave as a follow-up rather than block this decision logic on it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_common import MetropyError

from .graph import build_step_graph, topological_order
from .steps import PopulationStep, Step

if TYPE_CHECKING:
    from .config import Config
    from .file import MetroFile

# (source_config, the Step instance resolved against it, whether that copy is currently fresh).
SourceInfo = tuple["Config", Step, bool]


def _step_key(step: Step) -> tuple[type[Step], str | None]:
    """A key identifying "the same Step" across configs of a chain: its class, plus the
    population it was resolved for (`None` for a plain, non-`PopulationStep` Step).
    """
    if isinstance(step, PopulationStep):
        return (type(step), step.population_name)
    return (type(step), None)


def compute_source_config(
    config: Config,
    step_classes: list[type[Step]],
    steps: dict[Step, dict[str, set[MetroFile]]],
    generated_files: dict[MetroFile, set[Step]],
) -> dict[Step, Config]:
    """Returns, for each Step of `steps` (already resolved and feasibility-checked against
    `config`, as done by `MetroPipeline`), the Config whose `main_directory` holds the
    authoritative, up-to-date copy of that Step's output.
    """
    info = _compute_source_info(config, step_classes, steps, generated_files)
    return {step: source_config for step, (source_config, _step, _fresh) in info.items()}


def _compute_source_map(config: Config, step_classes: list[type[Step]]) -> dict[Step, SourceInfo]:
    """Same as `_compute_source_info`, but builds `config`'s own step graph first: used to
    recurse into an ancestor, which `MetroPipeline` has not already built a graph for.
    """
    graph = build_step_graph(config, step_classes)
    return _compute_source_info(config, step_classes, graph.steps, graph.generated_files)


def _compute_source_info(
    config: Config,
    step_classes: list[type[Step]],
    steps: dict[Step, dict[str, set[MetroFile]]],
    generated_files: dict[MetroFile, set[Step]],
) -> dict[Step, SourceInfo]:
    if config.parent_config is None:
        # Nothing to inherit from: `config` is authoritative for its own steps, whether or not
        # its own cache is currently fresh (a descendant checks that itself before trusting it).
        return {step: (config, step, not step.update_required()) for step in steps}
    parent_map = _parent_source_map(config.parent_config, step_classes)
    parent_by_key = {_step_key(anc_step): (anc_step, info) for anc_step, info in parent_map.items()}
    producer_of: dict[MetroFile, Step] = {f: next(iter(s)) for f, s in generated_files.items()}
    info: dict[Step, SourceInfo] = {}
    for step in topological_order(steps, generated_files):
        info[step] = _resolve_one_step(step, steps[step], config, parent_by_key, producer_of, info)
    return info


def _parent_source_map(
    parent_config: Config, step_classes: list[type[Step]]
) -> dict[Step, SourceInfo]:
    """Builds the parent's own source map, defensively: a config/parameter error while probing an
    ancestor must not crash the run being planned for `config` itself.
    """
    try:
        return _compute_source_map(parent_config, step_classes)
    except MetropyError:
        config_name = parent_config.main_path.name if parent_config.main_path else ""
        logger.warning(f"Parent config {config_name} is invalid, it will not be reused.")
        return {}


def _resolve_one_step(
    step: Step,
    spec: dict[str, set[MetroFile]],
    config: Config,
    parent_by_key: dict[tuple[type[Step], str | None], tuple[Step, SourceInfo]],
    producer_of: dict[MetroFile, Step],
    info: dict[Step, SourceInfo],
) -> SourceInfo:
    if str(step) == "LinearScheduleStep":
        breakpoint()
    own: SourceInfo = (config, step, not step.update_required())
    match = parent_by_key.get(_step_key(step))
    if match is None:
        # No equivalent Step in the parent (not defined there, not feasible there, or the
        # population is only declared by `config`'s own chain segment).
        return own
    ancestor_step, (ancestor_source_config, ancestor_source_step, ancestor_fresh) = match
    if not ancestor_fresh or step.config_hash() != ancestor_step.config_hash():
        return own
    # Every input file this Step reads must, in `config`'s own graph, come from a Step which is
    # itself sourced from the exact same ancestor: `config_hash()` only covers `step`'s own
    # declared Parameters, not the content of its upstream inputs.
    optional_inputs_generated = spec["optional_inputs"] & producer_of.keys()
    for input_file in spec["required_inputs"] | optional_inputs_generated:
        producer = producer_of[input_file]
        if info[producer][0] is not ancestor_source_config:
            return own
    return (ancestor_source_config, ancestor_source_step, True)
