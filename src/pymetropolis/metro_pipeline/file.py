from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, override

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

from pymetropolis.metro_common.errors import MetropyError, error_context

if TYPE_CHECKING:
    pass


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

    def __str__(self) -> str:
        if self == MetroDataType.ID:
            return "string or integer"
        elif self == MetroDataType.BOOL:
            return "boolean"
        elif self == MetroDataType.INT:
            return "integer"
        elif self == MetroDataType.UINT:
            return "unsigned integer"
        elif self == MetroDataType.FLOAT:
            return "float"
        elif self == MetroDataType.STRING:
            return "string"
        elif self == MetroDataType.TIME:
            return "time"
        elif self == MetroDataType.DURATION:
            return "duration"
        elif self == MetroDataType.LIST_OF_IDS:
            return "list of strings or integers"
        elif self == MetroDataType.LIST_OF_FLOATS:
            return "list of floats"
        elif self == MetroDataType.LIST_OF_TIMES:
            return "list of times"
        else:
            return "unspecified datatype"


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
            logger.warning(
                f"Invalid dtype for column `{self.name}`: {df[self.name].dtype} (expected: {self.dtype})"
            )
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

    def _md_doc(self) -> str:
        doc = f"| `{self.name}` | {self.dtype} | "
        for b in (self.optional, self.nullable, self.unique):
            if b:
                doc += "✓"
            else:
                doc += "✕"
            doc += " | "
        if self.description:
            doc += f"{self.description}"
        doc += " |"
        return doc


class MetroFile:
    path: str
    description: str = ""
    complete_path: Path

    def __str__(self) -> str:
        return self.__class__.__name__

    @classmethod
    def from_dir(cls, main_directory: Path) -> "MetroFile":
        instance = cls.__new__(cls)
        instance.complete_path = main_directory / Path(cls.path)
        instance.create_dir_if_needed()
        return instance

    def read(self) -> Any:
        raise MetropyError("Unimplemented")

    def write(self, value: Any):
        raise MetropyError("Unimplemented")

    def create_dir_if_needed(self):
        self.complete_path.parent.mkdir(exist_ok=True, parents=True)

    def exists(self) -> bool:
        return self.complete_path.exists()

    def get_path(self) -> Path:
        return self.complete_path

    def last_modified_time(self) -> int | float:
        return self.complete_path.stat().st_mtime

    def remove(self):
        self.complete_path.unlink()

    @classmethod
    def _md_doc(cls) -> str:
        doc = f"## {cls.__name__}\n\n"
        doc += f"{cls.description}\n\n"
        doc += f"- **Path:** `{cls.path}`\n"
        return doc

    @classmethod
    def _md_doc_schema(cls, simple: bool = True) -> str:
        return ""


class MetroDataFrameFile(MetroFile):
    schema: Optional[list[Column]] = None
    max_rows: Optional[int] = None

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        if self.max_rows is not None and len(df) > self.max_rows:
            raise MetropyError("DataFrame has too many rows")
        if self.schema is None:
            return df
        if not all(col.validate_df(df) for col in self.schema):
            raise MetropyError("DataFrame is not valid")
        for col in df.columns:
            if not any(col == c.name for c in self.schema):
                logger.warning(f"Discarding extra column: {col}")
                df = df.drop(col)
        return df

    @override
    @error_context(msg="Cannot save DataFrame {}", fmt_args=[0])
    def write(self, df: pl.DataFrame):
        df = self.validate(df)
        df.write_parquet(self.complete_path)

    def read(self) -> pl.DataFrame:
        return pl.read_parquet(self.complete_path)

    def read_if_exists(self) -> pl.DataFrame | None:
        if self.exists():
            return self.read()

    @override
    @classmethod
    def _md_doc(cls) -> str:
        doc = super()._md_doc()
        doc += "- **Type:** DataFrame\n"
        if cls.max_rows:
            doc += f"- **Max rows:** {cls.max_rows}\n"
        return doc

    @override
    @classmethod
    def _md_doc_schema(cls, simple: bool = True) -> str:
        doc = ""
        if cls.schema:
            if simple:
                doc += "- **Columns:**\n\n"
            else:
                doc += "<details>\n<summary>Show columns</summary>\n\n"
            doc += "| Column | Data type | Optional? | Nullable? | Unique? | Description |\n"
            doc += "| ------ | --------- | --------- | --------- | ------- | ----------- |\n"
            for col in cls.schema:
                doc += f"{col._md_doc()}\n"
            if not simple:
                doc += "\n</details>\n"
        return doc


class MetroGeoDataFrameFile(MetroFile):
    schema: Optional[list[Column]] = None
    max_rows: Optional[int] = None

    def validate(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        if self.max_rows is not None and len(gdf) > self.max_rows:
            raise MetropyError("DataFrame has too many rows")
        if self.schema is None:
            return gdf
        if not all(col.validate_gdf(gdf) for col in self.schema):
            raise MetropyError("GeoDataFrame is not valid")
        for col in gdf.columns:
            if col == "geometry":
                continue
            if not any(col == c.name for c in self.schema):
                logger.warning(f"Discarding extra column: {col}")
                gdf.drop(columns=col, inplace=True)
        return gdf

    @error_context(msg="Cannot save GeoDataFrame {}", fmt_args=[0])
    def write(self, gdf: gpd.GeoDataFrame):
        gdf = self.validate(gdf)
        gdf.to_parquet(self.complete_path)

    def read(self) -> gpd.GeoDataFrame:
        return gpd.read_parquet(self.complete_path)

    def read_if_exists(self) -> gpd.GeoDataFrame | None:
        if self.exists():
            return self.read()

    @override
    @classmethod
    def _md_doc(cls) -> str:
        doc = super()._md_doc()
        doc += "- **Type:** GeoDataFrame\n"
        if cls.max_rows:
            doc += f"- **Max rows:** {cls.max_rows}\n"
        return doc

    @override
    @classmethod
    def _md_doc_schema(cls, simple: bool = True) -> str:
        doc = ""
        if cls.schema:
            if simple:
                doc += "- **Columns:**\n"
            else:
                doc += "<details>\n<summary>Show columns</summary>\n\n"
            doc += "| Column | Data type | Optional? | Nullable? | Unique? | Description |\n"
            doc += "| ------ | --------- | --------- | --------- | ------- | ----------- |\n"
            for col in cls.schema:
                doc += f"{col._md_doc()}\n"
            if not simple:
                doc += "\n</details>\n"
        return doc


class MetroTxtFile(MetroFile):
    @error_context(msg="Cannot save Txt file {}", fmt_args=[0])
    def write(self, txt: str):
        with open(self.complete_path, "w") as f:
            f.write(txt)

    def read(self) -> str:
        with open(self.complete_path, "r") as f:
            return f.read()

    def read_if_exists(self) -> str | None:
        if self.exists():
            return self.read()

    @override
    @classmethod
    def _md_doc(cls) -> str:
        doc = super()._md_doc()
        doc += "- **Type:** Text\n"
        return doc


class MetroPlotFile(MetroFile):
    @error_context(msg="Cannot save plot {}", fmt_args=[0])
    def write(self, fig: plt.Figure):
        fig.savefig(self.complete_path, dpi=300)

    @override
    @classmethod
    def _md_doc(cls) -> str:
        doc = super()._md_doc()
        doc += "- **Type:** Plot\n"
        return doc
