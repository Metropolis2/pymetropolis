from importlib.metadata import version

import polars as pl

from .cli import app as app
from .main import main as main

__version__ = version("pymetropolis")

# Register the "geoarrow.wkb" extension so that polars does not send a warning when importing
# geoparquet files.
pl.register_extension_type("geoarrow.wkb", ext_class=pl.Extension)
