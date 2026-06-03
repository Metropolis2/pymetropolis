from __future__ import annotations

import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

from .errors import MetropyError

if TYPE_CHECKING:
    import polars as pl


def get_pl_expr(x: str | pl.Expr) -> pl.Expr:
    import polars as pl

    if isinstance(x, str):
        return pl.col(x)
    else:
        assert isinstance(x, pl.Expr)
        return x


def pl_duration_to_seconds(x: str | pl.Expr) -> pl.Expr:
    expr = get_pl_expr(x)
    return expr.dt.total_seconds(fractional=True)


def seconds_since_midnight_to_time_string(v: float) -> str:
    hours = int(v // 3600)
    minutes = int(v % 3600 // 60)
    seconds = int(v % 60 // 1)
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def seconds_to_duration_string(v: float) -> str:
    if v == 0.0:
        return "0"
    if v < 0.0:
        raise MetropyError("Invalid duration: {v}")
    if v >= 10:
        # Decimals will not be show so the value can be rounded.
        v = round(v)
    string = ""
    hours = int(v // 3600)
    minutes = int(v % 3600 // 60)
    seconds = int(v % 60 // 1)
    mls = int(v % 1 // 1e-3)
    mis = int(v % 1e-3 // 1e-6)
    ns = int(v % 1e-6 // 1e-9)
    if hours:
        string += f"{hours}h"
    if minutes:
        string += f"{minutes}m"
    if seconds:
        string += f"{seconds}s"
    if v < 10:
        # Small time. Display milliseconds.
        if mls:
            string += f"{mls}ms"
        if v < 1e-2:
            # Display microseconds.
            if mis:
                string += f"{mis}μs"
            if v < 1e-5:
                # Display nanoseconds.
                if ns:
                    string += f"{ns}ns"
    return string


@contextmanager
def tmp_download(url):
    """Download a file temporarily and remove it after use."""
    import requests

    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        logger.debug(f"Downloading file from url `{url}`")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(tmp_file.name, "wb") as f:
            shutil.copyfileobj(response.raw, f)
        try:
            yield tmp_file.name
        finally:
            try:
                os.remove(tmp_file.name)
            except OSError:
                logger.warning(f"Could not remove temporary filename `{tmp_file.name}`")
                pass


def find_file(pattern: str, directory: Path, recursive: bool = False):
    """Returns the first file that matches a pattern in the directory.

    Returns None if there is no file matching the pattern.
    """
    if recursive:
        return next(directory.rglob(pattern), None)
    else:
        return next(directory.glob(pattern), None)
