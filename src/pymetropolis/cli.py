from pathlib import Path
from typing import Annotated, Optional

import typer

import pymetropolis

from .metro_pipeline import Config, run_pipeline
from .schema import STEPS


def version_callback(value: bool):
    if value:
        print(pymetropolis.__version__)
        raise typer.Exit()


def app(
    config: Annotated[Path, typer.Argument(help="Path to the TOML configuration path to be used.")],
    # step: Annotated[
    #     Optional[str],
    #     typer.Argument(help="Slug of the step to be run."),
    # ],
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show the step that will be run without actually running them."
    ),
    version: Annotated[
        Optional[bool],
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
    metro_config = Config.from_toml(config)
    run_pipeline(metro_config, STEPS, dry_run=dry_run)
