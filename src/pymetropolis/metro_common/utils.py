import datetime
import os
import shutil
import tempfile
from contextlib import contextmanager

import polars as pl
import requests
from loguru import logger


def time_to_seconds_since_midnight(t: datetime.time) -> int:
    return t.hour * 3600 + t.minute * 60 + t.second


def time_to_seconds_since_midnight_pl(expr: pl.Expr) -> pl.Expr:
    return (
        expr.dt.hour().cast(pl.UInt32) * 3600
        + expr.dt.minute().cast(pl.UInt32) * 60
        + expr.dt.second().cast(pl.UInt32)
    ).cast(pl.Float64)


def seconds_since_midnight_to_time_pl(col: str) -> pl.Expr:
    return pl.time(
        hour=pl.col(col) // 3600,
        minute=pl.col(col) % 3600 // 60,
        second=pl.col(col) % 60,
        microsecond=pl.col(col) % 1 * 1_000_000,
    )


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
