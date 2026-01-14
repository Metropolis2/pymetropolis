import os

import geopandas as gpd
import polars as pl
from loguru import logger
from pyogrio.errors import DataSourceError

from .errors import MetropyError, error_context


def scan_dataframe(filename: str, **kwargs):
    """Scan a DataFrame from a Parquet or CSV file."""
    if not os.path.isfile(filename):
        raise MetropyError(f"File not found: `{filename}`")
    if filename.endswith(".parquet"):
        lf = pl.read_parquet(filename, use_pyarrow=True, **kwargs).lazy()
    elif filename.endswith(".csv"):
        lf = pl.scan_csv(filename, **kwargs)
    else:
        raise MetropyError(f"Unsupported format for input file: `{filename}`")
    return lf


def read_dataframe(filename: str, columns=None, **kwargs):
    """Reads a DataFrame from a Parquet or CSV file."""
    lf = scan_dataframe(filename, **kwargs)
    if columns is not None:
        lf = lf.select(columns)
    return lf.collect()


@error_context(msg="Cannot read `{}` as geodataframe", fmt_args=[0])
def read_geodataframe(filename: str, columns=None):
    """Reads a GeoDataFrame from a Parquet file or any other format supported by GeoPandas."""
    if not os.path.isfile(filename):
        raise MetropyError(f"File not found: `{filename}`")
    if filename.endswith(".parquet") or filename.endswith(".geoparquet"):
        gdf = gpd.read_parquet(filename, columns=columns)
    else:
        try:
            gdf = gpd.GeoDataFrame(gpd.read_file(filename, columns=columns, engine="pyogrio"))
        except DataSourceError:
            raise MetropyError(f"Unsupported format for input file: `{filename}`")
    if gdf.crs is None:
        # Assume that CRS is EPSG:4326 when unspecified.
        gdf.set_crs("EPSG:4326", inplace=True)
    missing_col = False
    if columns is not None:
        for col in columns:
            if col not in gdf.columns:
                missing_col = True
                logger.error(f"Missing column `{col}` in `{filename}`")
    if missing_col:
        raise MetropyError(f"Missing columns in `{filename}`")
    return gdf
