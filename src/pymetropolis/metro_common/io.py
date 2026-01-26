from pathlib import Path

import geopandas as gpd
import polars as pl
from loguru import logger
from pyogrio.errors import DataSourceError

from .errors import MetropyError, error_context


def scan_dataframe(filename: Path, **kwargs):
    """Scan a DataFrame from a Parquet or CSV file."""
    if not filename.exists():
        raise MetropyError(f"File not found: `{filename}`")
    if filename.suffix == ".parquet":
        lf = pl.read_parquet(filename, use_pyarrow=True, **kwargs).lazy()
    elif filename.suffix == ".csv":
        lf = pl.scan_csv(filename, **kwargs)
    else:
        raise MetropyError(f"Unsupported format for input file: `{filename}`")
    return lf


def read_dataframe(filename: Path, columns=None, **kwargs):
    """Reads a DataFrame from a Parquet or CSV file."""
    lf = scan_dataframe(filename, **kwargs)
    if columns is not None:
        lf = lf.select(columns)
    return lf.collect()


@error_context(msg="Cannot read `{}` as geodataframe", fmt_args=[0])
def read_geodataframe(filename: Path, columns=None):
    """Reads a GeoDataFrame from a Parquet file or any other format supported by GeoPandas."""
    if not filename.exists():
        raise MetropyError(f"File not found: `{filename}`")
    if filename.suffix == ".parquet" or filename.suffix == ".geoparquet":
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
