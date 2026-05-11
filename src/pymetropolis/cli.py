from pathlib import Path
from typing import Annotated

import typer

import pymetropolis

from .metro_pipeline import Config, MetroPipeline
from .schema import STEPS


def version_callback(value: bool):
    if value:
        print(pymetropolis.__version__)
        raise typer.Exit()


def app(
    config: Annotated[Path, typer.Argument(help="Path to the TOML configuration path to be used.")],
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show the step that will be run without actually running them."
    ),
    step_by_step: bool = typer.Option(
        False,
        "--step-by-step",
        help="Run steps one at a time, asking for a confirmation between each step.",
    ),
    step: Annotated[str | None, typer.Option(help="Explicitly ask for a step to be run.")] = None,
    version: Annotated[
        bool | None,
        typer.Option(
            "--version",
            help="Show Pymetropolis version and exit",
            callback=version_callback,
            is_eager=True,
        ),
    ] = None,
):
    """Python command line tool to generate, calibrate, run and analyse a METROPOLIS2 simulation."""
    # TODO command to list available steps
    config = Config.from_toml(config)
    pipeline = MetroPipeline(config, STEPS, target_step=step)
    pipeline.run(dry_run, step_by_step)
