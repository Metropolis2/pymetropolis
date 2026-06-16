import csv
from pathlib import Path

from loguru import logger

from .errors import MetropyError, error_context


def detect_csv_delimiter(filename: Path) -> str:
    """Guess the delimiter used for a CSV file."""
    with open(filename) as f:
        first_line = f.readline()
    sniffer = csv.Sniffer()
    return sniffer.sniff(first_line).delimiter


def scan_dataframe(filename: Path, **kwargs):
    """Scan a DataFrame from a Parquet or CSV file."""
    import polars as pl

    if pl.get_extension_type("geoarrow.wkb") is None:
        # Register the "geoarrow.wkb" extension so that polars does not send a warning when
        # importing geoparquet files.
        pl.register_extension_type("geoarrow.wkb", ext_class=pl.Extension)

    if not filename.exists():
        raise MetropyError(f"File not found: `{filename}`")
    if filename.suffix == ".parquet" or filename.suffix == ".geoparquet":
        lf = pl.read_parquet(filename, use_pyarrow=True, **kwargs).lazy()
    elif filename.suffix == ".csv":
        sep = detect_csv_delimiter(filename)
        lf = pl.scan_csv(filename, separator=sep, **kwargs)
    else:
        raise MetropyError(f"Unsupported format for input file: `{filename}`")
    return lf


def read_dataframe(filename: Path, columns=None, **kwargs):
    """Reads a DataFrame from a Parquet or CSV file."""
    from polars.exceptions import ColumnNotFoundError

    lf = scan_dataframe(filename, **kwargs)
    if columns is not None:
        try:
            lf = lf.select(columns)
        except ColumnNotFoundError:
            cols = lf.collect_schema().columns
            missing_cols = set(columns).difference(set(cols))
            missing_cols_str = ", ".join(map(lambda c: f"`{c}`", missing_cols))
            raise MetropyError(f"Columns {missing_cols_str} are missing from file `{filename}`")
    return lf.collect()


@error_context(msg="Cannot read `{}` as geodataframe", fmt_args=[0])
def read_geodataframe(filename: Path, columns=None):
    """Reads a GeoDataFrame from a Parquet file or any other format supported by GeoPandas."""
    import geopandas as gpd
    from pyogrio.errors import DataSourceError

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
