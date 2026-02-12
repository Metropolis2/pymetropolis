import datetime
import os
import shutil
import tempfile
from contextlib import contextmanager

import polars as pl
import requests
from loguru import logger

from .errors import MetropyError


def time_to_seconds_since_midnight(t: datetime.time) -> int:
    return t.hour * 3600 + t.minute * 60 + t.second


def get_pl_expr(x: str | pl.Expr) -> pl.Expr:
    if isinstance(x, str):
        return pl.col(x)
    else:
        assert isinstance(x, pl.Expr)
        return x


def time_to_seconds_since_midnight_pl(x: str | pl.Expr) -> pl.Expr:
    expr = get_pl_expr(x)
    return (
        expr.dt.hour().cast(pl.UInt32) * 3600
        + expr.dt.minute().cast(pl.UInt32) * 60
        + expr.dt.second().cast(pl.UInt32)
    ).cast(pl.Float64)


def seconds_since_midnight_to_time_pl(x: str | pl.Expr) -> pl.Expr:
    expr = get_pl_expr(x)
    return pl.time(
        hour=expr // 3600,
        minute=expr % 3600 // 60,
        second=expr % 60,
        microsecond=expr % 1 * 1_000_000,
    )


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
