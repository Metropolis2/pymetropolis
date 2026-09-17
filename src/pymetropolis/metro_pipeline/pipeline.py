import importlib.util
import sys
import time
from enum import Enum
from pathlib import Path

import click
import humanize
from loguru import logger
from termcolor import colored

from pymetropolis.metro_common import MetropyError

from .config import Config
from .dot import build_dot, render_dot
from .file import MetroFile
from .graph import file_key, instantiate_step_graph, rebase_step_graph, resolve_feasible_graph
from .reuse import compute_source_config
from .steps import Step

UP_TO_DATE_COLOR = (120, 120, 120)
INVALIDATED_COLOR = (230, 160, 0)
OUTDATED_COLOR = (220, 40, 40)
REUSE_COLOR = (100, 170, 220)


class StepStatus(Enum):
    # Step has already be run, its config did not change, the input files did not change.
    UP_TO_DATE = 0
    # Step has never be run or its config changed or it is the target step.
    OUTDATED = 1
    # Step has already be run and its config did not change, but an input file might change due to
    # an outdated step upstream.
    INVALIDATED = 2


class MetroPipeline:
    # List of defined steps, with their required input files, optional input files and output files.
    steps: dict[Step, dict[str, set[MetroFile]]]
    # List of files that can be generated, with the Step(s) that generate them.
    generated_files: dict[MetroFile, set[Step]]
    # List of files which are required or optional input for primary steps.
    # A step is "primary" if its priority is > 0.
    primary_input_files: set[MetroFile]
    # List of files which are required or optional input for the target step.
    target_input_files: set[MetroFile] = set()
    # Transitive closure of `primary_input_files` and `target_input_files`: also includes the
    # required/optional inputs of whichever (possibly non-primary) Step generates a needed file, so
    # that a chain of non-primary Steps feeding a primary Step only indirectly (through other
    # non-primary Steps) is entirely kept in the sequence, not just its last link.
    needed_files: set[MetroFile]
    config: Config
    target_step: Step | None = None
    # The Config whose `main_directory` canonically owns each Step's output: either `self.config`
    # itself, or (only possible when `self.config.parent_config` is set) an ancestor config
    # resolving that Step identically. See `reuse.py`. `self.steps`' file dicts are rebased against
    # this (see `graph.rebase_step_graph`), so a Step is actually read/written there, not
    # necessarily under `self.config.main_directory`.
    source_config: dict[Step, Config]

    def __init__(
        self, config: Config, step_classes: list[type[Step]], target_step: str | None = None
    ) -> None:
        self.config = config
        step_classes = self.load_custom_steps(step_classes)
        used_keys = set()
        for step_class in step_classes:
            assert issubclass(step_class, Step), f"Not a valid Step: {step_class}"
            # Keep track of all keys used.
            for _, p in step_class._iter_params():
                used_keys.add(str(p))
        # Instantiate every step against the config. Multiple steps are returned for
        # PopulationStep when multiple populations are defined in the config.
        self.steps, all_output_files = instantiate_step_graph(self.config, step_classes)
        # Every Step is instantiated by now, so every input data file has been hashed. Saved here
        # rather than at the end of `__init__` because `check_files_to_delete` below asks for a
        # confirmation and exits if it is denied, which would throw away the hashing work.
        self.config.digest_cache.save()
        self.config.check_unused_keys(used_keys)
        # Must run against the not-yet-feasibility-filtered `self.steps`, so that a target step
        # which turns out infeasible is diagnosed by `check_target_step_files` below (which lists
        # the specific missing input file) rather than reported as "unknown" here.
        self.find_target_step(target_step, step_classes)
        self.generated_files, self.primary_input_files = resolve_feasible_graph(self.steps)
        self.check_target_step_files()
        if self.config.parent_config is None:
            self.source_config = {step: self.config for step in self.steps}
        else:
            self.source_config = compute_source_config(
                self.config, step_classes, self.steps, self.generated_files
            )
        # Repoints a Step whose output canonically belongs to an ancestor config at that config's
        # `main_directory`, so that running an outdated such Step writes there instead of into
        # `self.config`'s own directory. This rebuilds `self.generated_files` and returns the
        # correspondingly rebuilt `primary_input_files` in place of the one `resolve_feasible_graph`
        # computed above, which would otherwise reference stale, pre-rebase file objects.
        self.primary_input_files = rebase_step_graph(
            self.steps, self.generated_files, self.source_config, self.config
        )
        # Only now: the file identities `set_target_input_files` reads off `self.steps[...]` would
        # otherwise be the stale, pre-rebase ones.
        self.set_target_input_files()
        self.compute_needed_files()
        self.check_files_to_delete(all_output_files)

    def load_custom_steps(self, step_classes: list[type[Step]]) -> list[type[Step]]:
        """Imports the Step subclasses defined in the user's `custom_steps` Python files (if any)
        and returns `step_classes` with them merged in.

        A custom Step whose name matches an existing Step's name (built-in, or from an
        earlier-listed custom file) replaces it, so that users can override a built-in Step with
        their own local-specific implementation.
        """
        if not self.config.custom_step_paths:
            return step_classes
        steps_by_name = {cls.__name__: cls for cls in step_classes}
        for i, path in enumerate(self.config.custom_step_paths):
            # Give each file a unique, synthetic module name: files are loaded straight from an
            # arbitrary path rather than imported as part of a package, so there is no "real"
            # dotted module name for them, and two custom files could otherwise share a stem
            # (e.g. two different `custom_steps.py` in different directories).
            module_name = f"_pymetropolis_custom_step_{i}_{path.stem}"
            # Build a module object from the file path without executing it yet.
            spec = importlib.util.spec_from_file_location(module_name, path)
            if spec is None or spec.loader is None:
                raise MetropyError(f"Could not load custom step file: `{path}`")
            module = importlib.util.module_from_spec(spec)
            # Register the module under its synthetic name before executing it, so that any class
            # defined in the file gets `__module__ == module_name` (this is what the check below
            # uses to tell "defined in this file" apart from "merely imported into this file",
            # e.g. the `Step` base class itself).
            sys.modules[module_name] = module
            # Actually run the file's code (imports, class definitions, ...).
            spec.loader.exec_module(module)
            for obj in vars(module).values():
                if (
                    isinstance(obj, type)
                    and issubclass(obj, Step)
                    and obj.__module__ == module_name
                ):
                    if obj.__name__ in steps_by_name:
                        logger.info(
                            f"Custom Step `{obj.__name__}` from `{path}` overrides an existing "
                            "Step with the same name"
                        )
                    steps_by_name[obj.__name__] = obj
        return list(steps_by_name.values())

    def check_files_to_delete(self, all_output_files: set[MetroFile]):
        to_delete_files = list()
        for ofile in sorted(all_output_files, key=file_key):
            f = ofile.from_dir(self.config.main_directory)
            if ofile not in self.generated_files and f.exists():
                to_delete_files.append(f)
        if to_delete_files:
            msg = "The following file(s) are not used anymore and will be removed:\n- "
            msg += "\n- ".join(str(f.get_path()) for f in to_delete_files)
            logger.warning(msg)
            if click.confirm("Continue?"):
                for f in to_delete_files:
                    f.remove()
            else:
                sys.exit()

    def find_target_step(self, target_step: str | None, step_classes: list[type[Step]]):
        if target_step is None:
            return
        # Try to find the target step in all the step classes.
        for step_class in step_classes:
            if step_class.__name__.lower() == target_step.lower():
                target_step_class = step_class
                break
        else:
            logger.error(f"Unknown Step: {target_step}")
            sys.exit()
        # Try to find the target step in the defined steps.
        for step in self.steps.keys():
            if step.__class__ == target_step_class:
                self.target_step = step
                break
        else:
            logger.error(
                f"Step {target_step} is not properly defined (missing configuration parameter?)"
            )
            sys.exit()

    def set_target_input_files(self):
        if self.target_step is None:
            return
        for f in (
            self.steps[self.target_step]["required_inputs"]
            | self.steps[self.target_step]["optional_inputs"]
        ):
            self.target_input_files.add(f)

    def compute_needed_files(self):
        """Computes `needed_files`, the transitive closure of `primary_input_files` and
        `target_input_files`.

        `primary_input_files` only holds the input files of primary Steps directly. A non-primary
        Step whose output is required only by *another non-primary* Step (itself feeding, possibly
        through further non-primary Steps, a primary Step) would not be recognized as needed from
        `primary_input_files` alone. This walks `generated_files` backward from the files already
        known to be needed, repeatedly pulling in the required/optional inputs of whichever Step
        generates each newly-needed file, until no new file is added.
        """
        self.needed_files = set(self.primary_input_files) | set(self.target_input_files)
        frontier = set(self.needed_files)
        while frontier:
            new_frontier: set[MetroFile] = set()
            for f in frontier:
                for s in self.generated_files.get(f, ()):
                    new_frontier |= (
                        self.steps[s]["required_inputs"] | self.steps[s]["optional_inputs"]
                    ) - self.needed_files
            self.needed_files |= new_frontier
            frontier = new_frontier

    def check_target_step_files(self):
        if self.target_step is not None and self.target_step not in self.steps:
            # At this point, target step is defined but it is not feasible because one of its input
            # file is not getting generated.
            errors = False
            for ifile in self.target_step._iter_resolved_input_files(required=True):
                if ifile not in self.generated_files:
                    errors = True
                    logger.error(
                        f"File {ifile} is required by Step {self.target_step}, but no defined step "
                        "can generate it"
                    )
            if errors:
                sys.exit()

    def find_sequence(self) -> list[tuple[Step, StepStatus]]:
        sequence = list()
        available_files = set()
        remaining = set(self.steps.keys())
        outdated_steps = set()
        to_run_steps = set()
        outdated_files = set()
        while True:
            # `remaining` is iterated in sorted order (rather than in `set` order, which depends on
            # object ids) so that steps which become runnable at the same time are always sequenced
            # in the same order from one run to the next.
            steps_to_add = [
                s
                for s in sorted(remaining, key=str)
                # Condition 1: all required files have already been generated.
                if self.steps[s]["required_inputs"].issubset(available_files)
                # Condition 2: all optional files *which will be generated* have already been
                # generated.
                and self.steps[s]["optional_inputs"]
                .intersection(self.generated_files)
                .issubset(available_files)
                # Condition 3: step is primary or one of its output file is needed for a primary
                # step (or the target step), possibly indirectly through other non-primary steps,
                # or it is the target step.
                and (
                    s.is_primary()
                    or any(f in self.needed_files for f in self.steps[s]["outputs"])
                    or s == self.target_step
                )
            ]
            if not steps_to_add:
                break
            for step in steps_to_add:
                status = StepStatus.UP_TO_DATE
                if step.update_required() or step == self.target_step:
                    status = StepStatus.OUTDATED
                    outdated_steps.add(step)
                    to_run_steps.add(step)
                    outdated_files.update(set(self.steps[step]["outputs"]))
                elif any(
                    f in outdated_files
                    for f in self.steps[step]["required_inputs"]
                    | self.steps[step]["optional_inputs"]
                ):
                    status = StepStatus.INVALIDATED
                    to_run_steps.add(step)
                    outdated_files.update(set(self.steps[step]["outputs"]))
                sequence.append((step, status))
                remaining.remove(step)
                available_files.update(set(self.steps[step]["outputs"]))
        # Check that all feasible *primary* steps were added to the sequence.
        remaining_primary = sorted(filter(lambda s: s.is_primary(), remaining), key=str)
        assert not remaining_primary, (
            "Some Steps could not be added to the sequence: "
            f"{', '.join(map(str, remaining_primary))}"
        )
        # Check that the target step is in the sequence.
        # (At this point, the pipeline should have already stopped if the target step is not run.)
        assert self.target_step is None or any(map(lambda x: x[0] == self.target_step, sequence))
        return sequence

    def run(
        self, dry_run: bool = False, step_by_step: bool = False, graph_path: Path | None = None
    ):
        sequence = self.find_sequence()
        if not sequence:
            logger.error("No Step to run.")
            return
        # Written before the steps are run, so that an unrenderable graph fails the run
        # immediately rather than after hours of simulation.
        if graph_path is not None:
            self.write_graph(sequence, graph_path)
        if dry_run:
            self.print_sequence(sequence)
        else:
            self.run_sequence(sequence, step_by_step=step_by_step)

    def write_graph(self, sequence: list[tuple[Step, StepStatus]], path: Path):
        """Saves a graph of the Steps to run, with their dependencies, to `path`."""
        reused = dict()
        for step, _ in sequence:
            source = self.source_config.get(step)
            if source is not None and source is not self.config:
                # The step actually reads/writes under `source`'s `main_directory`, not
                # `self.config`'s own (see `graph.rebase_step_graph`).
                reused[step] = source.main_path.name if source.main_path else ""
        source_dot = build_dot(
            [(step, status.name) for step, status in sequence],
            self.steps,
            self.generated_files,
            reused=reused,
        )
        render_dot(source_dot, path)
        logger.success(f"Pipeline graph saved to `{path}`")

    def print_sequence(self, sequence: list[tuple[Step, StepStatus]]):
        legend = ", ".join(
            colored(label, color, attrs=attrs)
            for label, color, attrs in (
                ("up to date", UP_TO_DATE_COLOR, []),
                ("outdated", OUTDATED_COLOR, ["bold"]),
                ("invalidated", INVALIDATED_COLOR, []),
            )
        )
        print(f"Legend: {legend}\n")
        s = ""
        for i, (step, status) in enumerate(sequence):
            attrs = list()
            match status:
                case StepStatus.UP_TO_DATE:
                    color = UP_TO_DATE_COLOR
                    tag = "up to date"
                case StepStatus.INVALIDATED:
                    color = INVALIDATED_COLOR
                    tag = "invalidated"
                case StepStatus.OUTDATED:
                    attrs.append("bold")
                    color = OUTDATED_COLOR
                    tag = "outdated"
            dep_str = colored(f"{i + 1}. {step} [{tag}]", color, attrs=attrs)
            source = self.source_config.get(step)
            if source is not None and source is not self.config:
                # The step above actually reads/writes under `source`'s `main_directory`, not
                # `self.config`'s own (see `graph.rebase_step_graph`).
                config_name = source.main_path.name if source.main_path else ""
                dep_str += colored(f" (from: {config_name})", REUSE_COLOR)
            s += dep_str + "\n"
        print(s)

    def run_sequence(self, sequence: list[tuple[Step, StepStatus]], step_by_step: bool = False):
        to_run_steps = list(filter(lambda x: x[1] != StepStatus.UP_TO_DATE, sequence))
        if to_run_steps:
            n = len(to_run_steps)
            for i, (step, _) in enumerate(to_run_steps):
                logger.info(f"=== Step {i + 1} / {n}: {step} ===")
                start = time.time()
                step.execute()
                end = time.time()
                logger.info(f"Done in {humanize.precisedelta(end - start)}")
                if step_by_step and i + 1 < len(to_run_steps):
                    next_step = to_run_steps[i + 1][0]
                    if click.confirm(f"Continue to next step? [{next_step}]"):
                        continue
                    else:
                        logger.success("Stopped!")
                        return
        else:
            logger.success("Nothing to do. All steps are still up-to-date!")
