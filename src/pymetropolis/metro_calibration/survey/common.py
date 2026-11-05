from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl


def restrict_to_home_in_area(df: pl.DataFrame, area) -> pl.DataFrame:
    """Restricts `df` to the rows whose home (columns `home_lng` and `home_lat`) falls within
    `area` (a shapely geometry in EPSG:4326). Rows without home coordinates are dropped."""
    import numpy as np
    import polars as pl
    import shapely

    lngs = df["home_lng"].cast(pl.Float64).fill_null(np.nan).to_numpy()
    lats = df["home_lat"].cast(pl.Float64).fill_null(np.nan).to_numpy()
    return df.filter(pl.Series(shapely.contains_xy(area, lngs, lats)))
