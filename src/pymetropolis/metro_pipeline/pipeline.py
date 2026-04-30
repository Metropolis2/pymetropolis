import sys
from collections import defaultdict
from enum import Enum

import click
from loguru import logger
from termcolor import colored

from pymetropolis.metro_common import logger as metro_logger

from .config import Config
from .file import MetroFile
from .steps import Step


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
    primary_input_files: set[MetroFile]
    config: Config
    target_step: Step | None = None

    def __init__(
        self, config: Config, step_classes: list[type[Step]], target_step: str | None = None
    ) -> None:
        metro_logger.setup()
        self.config = config
        steps = defaultdict(dict)
        all_output_files = set()
        used_keys = set()
        for step_class in step_classes:
            # Instantiate the step with the config.
            step = step_class(self.config)
            # Keep track of all keys used.
            for _, p in step_class._iter_params():
                used_keys.add(str(p))
            all_output_files.update(step.output_files.values())
            if step.is_defined() and step.output_files:
                steps[step]["required_inputs"] = set(
                    map(lambda f: f[1], step._iter_input_files(required=True))
                )
                steps[step]["optional_inputs"] = set(
                    map(lambda f: f[1], step._iter_input_files(required=False))
                )
                steps[step]["outputs"] = set(map(lambda f: f, step.output_files.values()))
        self.steps = steps
        self.check_unused_keys(used_keys)
        self.check_target_step_defined(target_step, step_classes)
        self.set_feasible()
        self.solve_conflicts()
        self.check_files_to_delete(all_output_files)

    def check_unused_keys(self, used_keys: set[str]):
        unused_keys = self.config.get_unused_keys(used_keys)
        if unused_keys:
            logger.warning("The following keys appear in the configuration but are not used:")
            for k in sorted(unused_keys):
                logger.warning(f"- {k}")

    def check_files_to_delete(self, all_output_files: set[MetroFile]):
        to_delete_files = list()
        for ofile in all_output_files:
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

    def check_target_step_defined(self, target_step: str | None, step_classes: list[type[Step]]):
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

    def set_feasible(self):
        self.generated_files = defaultdict(set)
        self.primary_input_files = set()
        remaining = set(self.steps.keys())
        while True:
            steps_to_add = {
                s
                for s in remaining
                if self.steps[s]["required_inputs"].issubset(self.generated_files)
            }
            if not steps_to_add:
                break
            remaining -= steps_to_add
            for s in steps_to_add:
                if s.primary:
                    for f in self.steps[s]["required_inputs"] | self.steps[s]["optional_inputs"]:
                        self.primary_input_files.add(f)
                for f in self.steps[s]["outputs"]:
                    self.generated_files[f].add(s)
        # Remove unfeasible steps from the step list.
        for s in remaining:
            self.steps.pop(s)
        self.check_target_step_files()

    def check_target_step_files(self):
        if self.target_step is not None and self.target_step not in self.steps:
            # At this point, target step is defined but it is not feasible because one of its input
            # file is not getting generated.
            errors = False
            for _, ifile in self.target_step._iter_input_files(required=True):
                if ifile not in self.generated_files:
                    errors = True
                    logger.error(
                        f"File {ifile.__name__} is required by Step {self.target_step}, but no "
                        "defined step can generate it"
                    )
            if errors:
                sys.exit()

    def find_next_conflict(self) -> set[Step] | None:
        for ofile, steps in self.generated_files.items():
            if len(steps) >= 2:
                steps_str = ", ".join(map(str, steps))
                logger.debug(
                    f"Multiple steps are generating file {ofile.__class__.__name__}: {steps_str}"
                )
                return steps

    def solve_conflicts(self):
        while True:
            conflict = self.find_next_conflict()
            if conflict is None:
                break
            to_remove_steps = self.least_priority_steps(conflict)
            steps_str = ", ".join(map(str, to_remove_steps))
            if len(to_remove_steps) > 1:
                logger.debug(f"Steps {steps_str} are discarded.")
            else:
                logger.debug(f"Step {steps_str} is discarded.")
            for s in to_remove_steps:
                self.steps.pop(s)
                self.set_feasible()

    def find_sequence(self) -> list[tuple[Step, StepStatus]]:
        sequence = list()
        available_files = set()
        remaining = set(self.steps.keys())
        outdated_steps = set()
        to_run_steps = set()
        outdated_files = set()
        while True:
            steps_to_add = [
                s
                for s in remaining
                # Condition 1: all required files have already been generated.
                if self.steps[s]["required_inputs"].issubset(available_files)
                # Condition 2: all optional files *which will be generated* have already been
                # generated.
                and self.steps[s]["optional_inputs"]
                .intersection(self.generated_files)
                .issubset(available_files)
                # Condition 3: step is primary or one of its output file is needed for a primary
                # step.
                and (
                    s.primary
                    or any(f in self.primary_input_files for f in self.steps[s]["outputs"])
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
        remaining_primary = list(filter(lambda s: s.primary, remaining))
        assert not remaining_primary, (
            "Some Steps could not be added to the sequence: "
            f"{', '.join(map(str, remaining_primary))}"
        )
        # Check that the target step is in the sequence.
        # (At this point, the pipeline should have already stopped if the target step is not run.)
        assert self.target_step is None or any(map(lambda x: x[0] == self.target_step, sequence))
        return sequence

    def least_priority_steps(self, conflict: set[Step]) -> set[Step]:
        """Returns the Steps with the least priority from a set of Steps.

        An ordering needs to be defined for the Step type (functions __lt__ and __eq__).
        """
        assert len(conflict) > 1
        ordered_steps = list(sorted(conflict))
        return set(ordered_steps[:-1])

    def run(self, dry_run: bool = False):
        sequence = self.find_sequence()
        if dry_run:
            self.print_sequence(sequence)
        else:
            self.run_sequence(sequence)

    def print_sequence(self, sequence: list[tuple[Step, StepStatus]]):
        s = ""
        for i, (step, status) in enumerate(sequence):
            attrs = list()
            match status:
                case StepStatus.UP_TO_DATE:
                    # attrs.append("strike")
                    color = "green"
                case StepStatus.INVALIDATED:
                    color = "yellow"
                case StepStatus.OUTDATED:
                    attrs.append("bold")
                    color = "red"
            dep_str = colored(f"{i + 1}. {step}", color, attrs=attrs)
            s += dep_str + "\n"
        print(s)
        # TODO: Plot a graph of the pipeline.

    def run_sequence(self, sequence: list[tuple[Step, StepStatus]]):
        to_run_steps = list(filter(lambda x: x[1] != StepStatus.UP_TO_DATE, sequence))
        if to_run_steps:
            n = len(to_run_steps)
            for i, (step, _) in enumerate(to_run_steps):
                logger.info(f"=== Step {i + 1} / {n}: {step} ===")
                step.execute(self.config)
        else:
            logger.success("Nothing to do. All steps are still up-to-date!")
