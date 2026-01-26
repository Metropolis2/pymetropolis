from pathlib import Path
from typing import Annotated

import typer

from .metro_pipeline import Config, run_pipeline
from .schema import STEPS


def app(
    config: Annotated[Path, typer.Argument(help="Path to the TOML configuration path to be used.")],
    # step: Annotated[
    #     Optional[str],
    #     typer.Argument(help="Slug of the step to be run."),
    # ],
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show the step that will be run without actually running them."
    ),
):
    """Python command line tool to generate, calibrate, run and analyse a METROPOLIS2 simulation."""
    # TODO command to list available steps
    metro_config = Config.from_toml(config)
    run_pipeline(metro_config, STEPS, dry_run=dry_run)
