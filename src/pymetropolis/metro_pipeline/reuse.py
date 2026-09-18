"""Decides, for each Step of a config with a `parent_config`, which config's `main_directory`
canonically owns that Step's output.

This module only computes *where* a Step's output belongs (`compute_source_config`); it is
`graph.rebase_step_graph` that actually repoints a Step's files there, and whether that location
is currently fresh (i.e. whether the Step still needs to run) is then just
`step.update_required()`, checked lazily at that location like any other Step. See `CLAUDE.md`
for the rationale (avoiding duplicate work/output across a chain of derived scenario configs).

A Step `s` resolved against config `C` (with parent `P`) canonically belongs to a config `S` (one
of `C.chain()`, `S != C`) when, walking from `P` up to `S`:

1. An equivalent Step exists in each config of the chain up to `S` (same class, same
   population) with an identical `config_hash()` — i.e. every `Parameter` the Step reads
   resolves to the same value there as in `C`.
2. Every required/optional input file `s` actually reads (per `C`'s own step graph) is produced,
   in that same graph, by a Step whose resolved source is the *same* as what that same file
   (matched by class, since a `MetroFile` subclass has one fixed `path`) resolves to in `P`'s own
   graph — `config_hash()` alone only covers `s`'s own declared `Parameter`s, not whether its
   upstream inputs match. This is deliberately a comparison against what the file resolves to in
   `P`, not against `s`'s own candidate source: `s`'s own Parameters can differ from a further
   ancestor (so `s` only matches as far up as `P`) while its inputs are still shared with that
   further ancestor (e.g. a Step whose schedule Parameters were overridden at `P`, but whose trip
   data is untouched all the way back to the root) — in that case the input still checks out.

Notably, this does *not* depend on whether `S`'s own copy is currently fresh: a Step can
canonically belong to a config that has never actually been run (or whose cache has gone stale),
in which case it still needs to be (re)computed, but the config that ends up owning it is `S`, not
`C` — running `C` then writes into `S`'s `main_directory` instead of `C`'s own.

A Step failing check 1 or 2 canonically belongs to `C` itself (today's behavior without a
`parent_config`): it is (re)computed in `C`'s own `main_directory`.

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

# (source_config, the Step instance resolved against it).
SourceInfo = tuple["Config", Step]
# For one Config's own graph: what each generated MetroFile class resolves to.
FileSourceMap = dict[type, SourceInfo]


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
    `config`, as done by `MetroPipeline`), the Config whose `main_directory` canonically owns
    that Step's output — see the module docstring for what "canonically owns" means.
    """
    info, _file_source = _compute_source_info(config, step_classes, steps, generated_files)
    return {step: source_config for step, (source_config, _step) in info.items()}


def _compute_source_map(
    config: Config, step_classes: list[type[Step]]
) -> tuple[dict[Step, SourceInfo], FileSourceMap]:
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
) -> tuple[dict[Step, SourceInfo], FileSourceMap]:
    if config.parent_config is None:
        # Nothing to inherit from: `config` is authoritative for its own steps.
        info: dict[Step, SourceInfo] = {step: (config, step) for step in steps}
    else:
        parent_info, parent_file_source = _parent_source_map(config.parent_config, step_classes)
        parent_by_key = {
            _step_key(anc_step): (anc_step, src) for anc_step, src in parent_info.items()
        }
        producer_of: dict[MetroFile, Step] = {f: next(iter(s)) for f, s in generated_files.items()}
        info = {}
        for step in topological_order(steps, generated_files):
            info[step] = _resolve_one_step(
                step, steps[step], config, parent_by_key, producer_of, info, parent_file_source
            )
    # This config's own per-file-type resolution, returned for whichever descendant recurses into
    # it next (see check 2 in the module docstring).
    file_source: FileSourceMap = {}
    for step, spec in steps.items():
        for f in spec["outputs"]:
            file_source[type(f)] = info[step]
    return info, file_source


def _parent_source_map(
    parent_config: Config, step_classes: list[type[Step]]
) -> tuple[dict[Step, SourceInfo], FileSourceMap]:
    """Builds the parent's own source map, defensively: a config/parameter error while probing an
    ancestor must not crash the run being planned for `config` itself.
    """
    try:
        return _compute_source_map(parent_config, step_classes)
    except MetropyError:
        config_name = parent_config.main_path.name if parent_config.main_path else ""
        logger.warning(f"Parent config {config_name} is invalid, it will not be reused.")
        return {}, {}


def _resolve_one_step(
    step: Step,
    spec: dict[str, set[MetroFile]],
    config: Config,
    parent_by_key: dict[tuple[type[Step], str | None], tuple[Step, SourceInfo]],
    producer_of: dict[MetroFile, Step],
    info: dict[Step, SourceInfo],
    parent_file_source: FileSourceMap,
) -> SourceInfo:
    own: SourceInfo = (config, step)
    match = parent_by_key.get(_step_key(step))
    if match is None:
        # No equivalent Step in the parent (not defined there, not feasible there, or the
        # population is only declared by `config`'s own chain segment).
        return own
    ancestor_step, (ancestor_source_config, ancestor_source_step) = match
    if step.config_hash() != ancestor_step.config_hash():
        return own
    # See check 2 in the module docstring: compare against what the parent's own graph resolves
    # this file to, not against `ancestor_source_config` (the *Step's* own candidate source) — a
    # Step's inputs can be shared with a further ancestor even when its own Parameters only match
    # as far up as the immediate parent.
    optional_inputs_generated = spec["optional_inputs"] & producer_of.keys()
    for input_file in spec["required_inputs"] | optional_inputs_generated:
        producer = producer_of[input_file]
        source_here = info[producer][0]
        ancestor_input_source = parent_file_source.get(type(input_file))
        if ancestor_input_source is None or source_here is not ancestor_input_source[0]:
            return own
    return (ancestor_source_config, ancestor_source_step)
