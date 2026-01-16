from loguru import logger
from termcolor import colored

from pymetropolis.metro_common import logger as metro_logger
from pymetropolis.metro_common.errors import MetropyError

from .config import Config
from .steps import Step


class RunOrder:
    def __init__(self, step: Step, config: Config):
        self.all_steps: set[str] = set()
        self.mandatory_steps: set[str] = set()
        self.undefined_steps: set[str] = set()
        self.order: list[Step] = list()
        self.add_dependency(step, config)

    def add_dependency(self, step: Step, config: Config, optional: bool = False):
        if step.slug in self.all_steps:
            # This step (and all its dependencies) have already been added to the run order.
            # If this step is part of the order, it means that an update is required.
            return step.slug in self.order
        if not step.is_defined(config):
            # The step is not defined so it cannot be properly executed.
            if optional:
                # The step is optional so it can simply be skipped.
                return False
            else:
                # The step is mandatory so we add it to the list of step that will not be able to
                # run.
                self.undefined_steps.add(step.slug)
        update_required = step.update_required(config)
        for dep in step.required_dependencies(config):
            # Recursively explore the step dependencies with a depth-first search.
            update_required |= self.add_dependency(dep, config)
        for dep in step.optional_dependencies(config):
            # Recursively explore the optional step dependencies with a depth-first search.
            update_required |= self.add_dependency(dep, config, optional=True)
        assert step.slug not in self.all_steps
        assert step.slug not in self.mandatory_steps
        self.all_steps.add(step.slug)
        if update_required:
            self.order.append(step)
            if not optional:
                self.mandatory_steps.add(step.slug)
        return update_required

    def pretty_order(self) -> str:
        s = ""
        for i, step in enumerate(self.order):
            dep_str = f"{i + 1}. {step.slug}"
            if step.slug in self.undefined_steps:
                dep_str = colored(f"{dep_str} *", "red")
            s += dep_str + "\n"
        if self.undefined_steps:
            s += colored("Steps in red with an asterisk (*) are not properly defined")
        return s


def run_pipeline(requested_step: Step, config: Config, dry_run: bool = False):
    metro_logger.setup()
    run_order = RunOrder(requested_step, config)
    if dry_run:
        print(run_order.pretty_order())
    else:
        if run_order.undefined_steps:
            for step in run_order.undefined_steps:
                logger.error(f"Step `{step}` is not properly defined")
            raise MetropyError("At least one step was not properly defined")
        n = len(run_order.order)
        for i, step in enumerate(run_order.order):
            logger.info(f"=== Step {i + 1} / {n}: {step.slug} ===")
            success = step.execute(config)
            if not success:
                # TODO. Send error.
                break
