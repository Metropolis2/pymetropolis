from __future__ import annotations

from datetime import date, timedelta
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, overload

from loguru import logger
from typing_extensions import TypeVar

from pymetropolis.metro_common.errors import error_context
from pymetropolis.metro_common.time import MetroTime

from .types import (
    Bool,
    CustomValidator,
    Date,
    Dict,
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
    from collections.abc import Iterator

    from .config import Config

T = TypeVar("T", default=Any)


class Key:
    """Class for lookup keys in the TOML configuration.

    A key "outer.inner" means the "inner" entry within the "outer" TOML section.

    The `_deprecated` attribute can be used for keys that should no longer be used.
    """

    _value: str
    _key_list: list[str]
    _deprecated: bool

    def __init__(self, key: str, deprecated: bool = False):
        self._value = key
        self._key_list = key.split(".")
        self._deprecated = deprecated

    def __str__(self) -> str:
        return self._value

    def __iter__(self) -> Iterator[str]:
        yield from self._key_list

    @property
    def deprecated(self) -> bool:
        return self._deprecated


class Parameter(Generic[T]):
    # Lookup keys for the parameter in the config.
    # The first key found is used for the parameter value.
    keys: list[Key]
    # Deprecated lookup keys (they still work but log a deprecation warning).
    deprecated_keys: list[Key]
    validator: Type
    description: str
    example: str
    note: str
    # When a parameter is "shared", its value is used across all extra populations, even though it
    # needs be defined only in the main configuration.
    shared: bool

    def __init__(
        self,
        keys: str | list[str],
        validator: Type,
        deprecated_keys: str | list[str] | None = None,
        default: Any = None,
        description: str = "",
        note: str = "",
        example: str = "",
        shared: bool = False,
    ):
        if not isinstance(keys, list):
            keys = [keys]
        self.keys = [Key(k) for k in keys]
        if not isinstance(deprecated_keys, list):
            deprecated_keys = [deprecated_keys] if deprecated_keys is not None else []
        self.deprecated_keys = [Key(k, deprecated=True) for k in deprecated_keys]
        self.validator = validator
        self.description = description
        self.note = note
        self.example = example
        self.shared = shared
        # Not validated here: `default` may itself be a `"secret:"` / `"env:"` indirection, which
        # can only be resolved against a `Config` instance, in `from_config` below.
        self.default = default

    def __str__(self) -> str:
        return str(self.keys[0])

    def _md_doc(self) -> str:
        key_str = str(self)
        doc = f"### `{key_str}`\n\n"
        if self.description:
            doc += f"- **Description:** {self.description}\n"
        doc += f"- **Allowed values:** {self.validator._describe()}\n"
        if len(self.keys) > 1:
            aliases = ", ".join([f"`{k}`" for k in self.keys[1:]])
            doc += f"- **Aliases:** {aliases}\n"
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

    def iter_keys(self, exclude_deprecated: bool = False) -> Iterator[Key]:
        """Iter all key aliases (in lookup order) for the parameter.

        Deprecated keys are included if `exclude_deprecated` is False.
        """
        if exclude_deprecated:
            return iter(self.keys)
        else:
            return chain(self.keys, self.deprecated_keys)

    def _anchor(self) -> str:
        """Returns the markdown URL anchor for the parameter (the first key, with dots removed)."""
        return str(self.keys[0]).replace(".", "")

    def nested(self) -> bool:
        """Returns True if the main key lookup is nested, i.e., it looks within a TOML section."""
        return len(list(self.keys[0])) > 1

    @error_context("Cannot validate parameter `{}`", fmt_args=[0])
    def from_config(self, config: Config, population_name: str | None = None) -> T | None:
        # Read parameter value from the config, or fall back to the default if no value is
        # specified. This must not cache the resolved value on `self`: a Parameter is a single
        # descriptor object shared (by inheritance) across every Step subclass and instance that
        # declares it, so any state stored on `self` would leak between unrelated steps and
        # populations.
        value, origin, key = config.resolve_parameter_with_origin(
            list(self.iter_keys()), population_name, shared=self.shared
        )
        if value is None:
            # No key was found.
            # Set the value to its default, resolving indirection in case the default is of the form
            # secrets:* or env:*.
            value = config.resolve_indirection(self.default)
            # A default comes from the code, not from a config file, so it is resolved against the
            # config being run rather than against whichever parent config was last consulted.
            origin = config
        elif key is not None and key.deprecated:
            # Log a warning for deprecated keys.
            logger.warning(f"Parameter key {key} is deprecated. Please use {self.keys[0]} instead.")
        if value is None:
            return None
        # Relative paths are resolved against the directory of the config the value was read from,
        # which is not necessarily `config` itself when it inherits from a parent config: a path
        # written in a parent config points to a file next to *that* config.
        value = self.validator.resolve(value, origin.resolve_path)
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
        check_file_or_dir_exists: bool = False,
        extensions: list[str] | None = None,
        **kwargs,
    ):
        kwargs["validator"] = PathType(
            check_file_exists=check_file_exists,
            check_dir_exists=check_dir_exists,
            check_file_or_dir_exists=check_file_or_dir_exists,
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


class DictParameter(Parameter[dict[str, Any] | None]):
    def __init__(self, *args, inner: Type, **kwargs):
        kwargs["validator"] = Dict(inner)
        super().__init__(*args, **kwargs)
