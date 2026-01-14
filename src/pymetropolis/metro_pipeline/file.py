import os
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional

import geopandas as gpd
import matplotlib.pyplot as plt
import polars as pl
from loguru import logger
from pandas.api.types import (
    is_bool_dtype,
    is_float_dtype,
    is_integer_dtype,
    is_string_dtype,
    is_unsigned_integer_dtype,
)

from pymetropolis.metro_common import MetropyError

from .config import Config

if TYPE_CHECKING:
    from .steps import Step


class MetroDataType(Enum):
    ID = 0
    BOOL = 1
    INT = 2
    UINT = 3
    FLOAT = 4
    STRING = 5
    TIME = 6
    DURATION = 7
    LIST_OF_IDS = 8
    LIST_OF_FLOATS = 9
    LIST_OF_TIMES = 10

    def is_valid_pl(self, dtype: pl.DataType):
        if self == MetroDataType.ID:
            return dtype.is_integer() or isinstance(dtype, pl.String)
        elif self == MetroDataType.BOOL:
            return isinstance(dtype, pl.Boolean)
        elif self == MetroDataType.INT:
            return dtype.is_integer()
        elif self == MetroDataType.UINT:
            return dtype.is_unsigned_integer()
        elif self == MetroDataType.FLOAT:
            return dtype.is_float()
        elif self == MetroDataType.STRING:
            return isinstance(dtype, pl.String)
        elif self == MetroDataType.TIME:
            return isinstance(dtype, pl.Time)
        elif self == MetroDataType.DURATION:
            return isinstance(dtype, pl.Duration)
        elif self == MetroDataType.LIST_OF_IDS:
            return isinstance(dtype, pl.List) and (
                dtype.inner.is_integer() or isinstance(dtype.inner, pl.String)
            )
        elif self == MetroDataType.LIST_OF_FLOATS:
            return isinstance(dtype, pl.List) and dtype.inner.is_float()
        elif self == MetroDataType.LIST_OF_TIMES:
            return isinstance(dtype, pl.List) and isinstance(dtype.inner, pl.Time)
        else:
            return False

    def is_valid_gdf(self, dtype: Any):
        if self == MetroDataType.ID:
            return is_integer_dtype(dtype) or is_string_dtype(dtype)
        elif self == MetroDataType.BOOL:
            return is_bool_dtype(dtype)
        elif self == MetroDataType.INT:
            return is_integer_dtype(dtype)
        elif self == MetroDataType.UINT:
            return is_unsigned_integer_dtype(dtype)
        elif self == MetroDataType.FLOAT:
            return is_float_dtype(dtype)
        elif self == MetroDataType.STRING:
            return is_string_dtype(dtype)
        # TIME and DURATION dtypes are not allowed in GeoDataFrames.
        else:
            return False


class Column:
    def __init__(
        self,
        name: str,
        dtype: MetroDataType,
        optional: bool = False,
        nullable: bool = True,
        unique: bool = False,
        description: Optional[str] = None,
    ):
        self.name = name
        self.dtype = dtype
        self.optional = optional
        self.nullable = nullable
        self.unique = unique
        self.description = description

    def validate_df(self, df: pl.DataFrame) -> bool:
        if not self.optional and self.name not in df.columns:
            logger.warning(f"Missing required column `{self.name}`")
            return False
        if self.name not in df.columns:
            return True
        if not self.dtype.is_valid_pl(df[self.name].dtype):
            logger.warning(f"Invalid dtype for column `{self.name}`: {df[self.name].dtype}")
            return False
        if not self.nullable and df[self.name].has_nulls():
            logger.warning(f"Column `{self.name}` has null values")
            return False
        if self.unique and df[self.name].n_unique() != len(df):
            logger.warning(f"Column `{self.name}` has duplicate values")
            return False
        return True

    def validate_gdf(self, gdf: gpd.GeoDataFrame) -> bool:
        if not self.optional and self.name not in gdf.columns:
            logger.warning(f"Missing required column `{self.name}`")
            return False
        if self.name not in gdf.columns:
            return True
        if not self.dtype.is_valid_gdf(gdf[self.name].dtype):
            logger.warning(f"Invalid dtype for column `{self.name}`: {gdf[self.name].dtype}")
            return False
        if not self.nullable and gdf[self.name].hasnans:
            logger.warning(f"Column `{self.name}` has null values")
            return False
        if self.unique and gdf[self.name].nunique() != len(gdf):
            logger.warning(f"Column `{self.name}` has duplicate values")
            return False
        return True


class MetroFile:
    def __init__(
        self,
        slug: str,
        path: str,
        description: Optional[str] = None,
    ):
        self.slug = slug
        self.path = path
        self.description = description
        self.providers = list()

    def add_provider(self, provider: "Step"):
        self.providers.append(provider)

    def get_path(self, config: Config):
        return config.path_from_main_dir(self.path)

    def provider(self, config: Config, optional: bool = False) -> Optional["Step"]:
        """Returns the Step that must be run to supply this file."""
        provider = None
        for step in self.providers:
            if step.is_defined(config):
                if provider is not None:
                    raise MetropyError(
                        f"Both `{provider.slug}` and `{step.slug}` Steps are providing "
                        f"MetroFile `{self.slug}`. "
                        "Only use one of them."
                    )
                provider = step
        if provider is None:
            # No properly defined provider was found.
            if optional:
                return None
            if len(self.providers) == 0:
                raise MetropyError(f"No provider exist for MetroFile `{self.slug}`")
            if len(self.providers) == 1:
                slug = self.providers[0].slug
                raise MetropyError(
                    f"The step {slug} must be defined to provide MetroFile `{self.slug}`"
                )
            else:
                slugs = ", ".join(f"`{p.slug}`" for p in self.providers)
                raise MetropyError(
                    f"There is no step defined to provide MetroFile `{self.slug}`. "
                    f"Define one of these steps: {slugs}"
                )
        return provider

    def create_dir_if_needed(self, config: Config):
        path = self.get_path(config)
        parentdir = os.path.dirname(path)
        if not os.path.isdir(parentdir):
            os.makedirs(parentdir)

    def exists(self, config: Config) -> bool:
        return os.path.isfile(self.get_path(config))


class MetroDataFrameFile(MetroFile):
    def __init__(
        self,
        *args,
        schema: Optional[list[Column]] = None,
        max_rows: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.schema = schema or list()
        self.max_rows = max_rows

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        if self.max_rows is not None and len(df) > self.max_rows:
            raise MetropyError("DataFrame has too many rows")
        if not all(col.validate_df(df) for col in self.schema):
            raise MetropyError("DataFrame is not valid")
        for col in df.columns:
            if not any(col == c.name for c in self.schema):
                logger.warning(f"Discarding extra column: {col}")
                df = df.drop(col)
        return df

    def save(self, df: pl.DataFrame, config: Config):
        df = self.validate(df)
        self.create_dir_if_needed(config)
        df.write_parquet(self.get_path(config))

    def read(self, config: Config) -> pl.DataFrame:
        return pl.read_parquet(self.get_path(config))

    def read_if_exists(self, config: Config) -> pl.DataFrame | None:
        if self.exists(config):
            return self.read(config)
        else:
            return None


class MetroGeoDataFrameFile(MetroFile):
    def __init__(
        self,
        *args,
        schema: Optional[list[Column]] = None,
        max_rows: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.schema = schema or list()
        self.max_rows = max_rows

    def validate(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        if not all(col.validate_gdf(gdf) for col in self.schema):
            raise MetropyError("GeoDataFrame is not valid")
        for col in gdf.columns:
            if col == "geometry":
                continue
            if not any(col == c.name for c in self.schema):
                logger.warning(f"Discarding extra column: {col}")
                gdf.drop(columns=col, inplace=True)
        return gdf

    def save(self, gdf: gpd.GeoDataFrame, config: Config):
        gdf = self.validate(gdf)
        self.create_dir_if_needed(config)
        gdf.to_parquet(self.get_path(config))

    def read(self, config: Config) -> gpd.GeoDataFrame:
        return gpd.read_parquet(self.get_path(config))

    def read_if_exists(self, config: Config) -> gpd.GeoDataFrame | None:
        if self.exists(config):
            return self.read(config)
        else:
            return None


class MetroTxtFile(MetroFile):
    def save(self, txt: str, config: Config):
        self.create_dir_if_needed(config)
        with open(self.get_path(config), "w") as f:
            f.write(txt)

    def read(self, config: Config) -> str:
        with open(self.get_path(config), "r") as f:
            return f.read()

    def read_if_exists(self, config: Config) -> str | None:
        if self.exists(config):
            return self.read(config)
        else:
            return None


class MetroPlotFile(MetroFile):
    def save(self, fig: plt.Figure, config: Config):
        self.create_dir_if_needed(config)
        fig.savefig(self.get_path(config))
