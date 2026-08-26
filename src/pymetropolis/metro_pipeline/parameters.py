from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, overload

from typing_extensions import TypeVar

from pymetropolis.metro_common.errors import error_context
from pymetropolis.metro_common.time import MetroTime

from .types import (
    Bool,
    CustomValidator,
    Date,
    Duration,
    Enum,
    ExecPathType,
    Float,
    Int,
    List,
    PathType,
    String,
    Time,
    Type,
)

if TYPE_CHECKING:
    from .config import Config

T = TypeVar("T", default=Any)


class Parameter(Generic[T]):
    key: list[str]
    validator: Type
    description: str
    example: str
    note: str
    # When a parameter is "shared", its value is used across all extra populations, even though it
    # needs be defined only in the main configuration.
    shared: bool

    def __init__(
        self,
        key: str,
        validator: Type,
        default: Any = None,
        description: str = "",
        note: str = "",
        example: str = "",
        shared: bool = False,
    ):
        self.key = key.split(".")
        self.validator = validator
        self.description = description
        self.note = note
        self.example = example
        self.shared = shared
        # Not validated here: `default` may itself be a `"secret:"` / `"env:"` indirection, which
        # can only be resolved against a `Config` instance, in `from_config` below.
        self.default = default

    def __str__(self) -> str:
        return ".".join(self.key)

    def _md_doc(self) -> str:
        key_str = ".".join(self.key)
        doc = f"### `{key_str}`\n\n"
        if self.description:
            doc += f"- **Description:** {self.description}\n"
        doc += f"- **Allowed values:** {self.validator._describe()}\n"
        if self.example:
            doc += f"- **Example:** {self.example}\n"
        if self.note:
            doc += f"- **Note:** {self.note}\n"
        return doc

    @overload
    def __get__(self, instance: None, owner: Any) -> Parameter[T]: ...

    @overload
    def __get__(self, instance: Any, owner: Any) -> T: ...

    def __get__(self, instance: Any, owner: Any) -> Any:
        return self

    @error_context("Cannot validate parameter `{}`", fmt_args=[0])
    def from_config(self, config: Config, population_name: str | None = None) -> T | None:
        # Read parameter value from the config, or fall back to the default if no value is
        # specified. This must not cache the resolved value on `self`: a Parameter is a single
        # descriptor object shared (by inheritance) across every Step subclass and instance that
        # declares it, so any state stored on `self` would leak between unrelated steps and
        # populations.
        value = config.resolve_parameter(self.key, population_name, shared=self.shared)
        if value is None:
            value = config.resolve_indirection(self.default)
        if value is None:
            return None
        value = self.validator.resolve(value, config.resolve_path)
        return self.validator.validate(value)


class CustomParameter(Parameter):
    def __init__(self, *args, validator, **kwargs):
        kwargs["validator"] = CustomValidator(
            fn=validator, description=kwargs.pop("validator_description")
        )
        super().__init__(*args, **kwargs)


class BoolParameter(Parameter[bool | None]):
    def __init__(self, *args, **kwargs):
        kwargs["validator"] = Bool()
        super().__init__(*args, **kwargs)


class IntParameter(Parameter[int | None]):
    def __init__(
        self, *args, lower_bound: int | None = None, upper_bound: int | None = None, **kwargs
    ):
        kwargs["validator"] = Int(lb=lower_bound, ub=upper_bound)
        super().__init__(*args, **kwargs)


class FloatParameter(Parameter[float | None]):
    def __init__(
        self, *args, lower_bound: float | None = None, upper_bound: float | None = None, **kwargs
    ):
        kwargs["validator"] = Float(lb=lower_bound, ub=upper_bound)
        super().__init__(*args, **kwargs)


class FractionParameter(FloatParameter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, lower_bound=0.0, upper_bound=1.0, **kwargs)


class StringParameter(Parameter[str | None]):
    def __init__(self, *args, **kwargs):
        kwargs["validator"] = String()
        super().__init__(*args, **kwargs)


class DateParameter(Parameter[date | None]):
    def __init__(self, *args, **kwargs):
        kwargs["validator"] = Date()
        super().__init__(*args, **kwargs)


class TimeParameter(Parameter[MetroTime | None]):
    def __init__(self, *args, **kwargs):
        kwargs["validator"] = Time()
        super().__init__(*args, **kwargs)


class DurationParameter(Parameter[timedelta | None]):
    def __init__(self, *args, **kwargs):
        kwargs["validator"] = Duration()
        super().__init__(*args, **kwargs)


class EnumParameter(Parameter[Any | None]):
    def __init__(self, *args, values: list[Any], **kwargs):
        kwargs["validator"] = Enum(values=values)
        super().__init__(*args, **kwargs)


class PathParameter(Parameter[Path | None]):
    def __init__(
        self,
        *args,
        check_file_exists: bool = False,
        check_dir_exists: bool = False,
        extensions: list[str] | None = None,
        **kwargs,
    ):
        kwargs["validator"] = PathType(
            check_file_exists=check_file_exists,
            check_dir_exists=check_dir_exists,
            extensions=extensions,
        )
        super().__init__(*args, **kwargs)


class ExecPathParameter(Parameter[Path | None]):
    def __init__(self, *args, **kwargs):
        kwargs["validator"] = ExecPathType()
        super().__init__(*args, **kwargs)


class ListParameter(Parameter[list[Any] | None]):
    def __init__(
        self,
        *args,
        inner: Type,
        length: int | None = None,
        min_length: int | None = None,
        max_length: int | None = None,
        **kwargs,
    ):
        kwargs["validator"] = List(inner, length, min_length, max_length)
        super().__init__(*args, **kwargs)
