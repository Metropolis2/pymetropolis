from datetime import time, timedelta
from pathlib import Path
from typing import Any, Generic, Optional, overload

from typing_extensions import TypeVar

from .config import Config
from .types import (
    Bool,
    CustomValidator,
    Duration,
    Enum,
    Float,
    Int,
    List,
    PathType,
    String,
    Time,
    Type,
)

T = TypeVar("T", default=Any)


class Parameter(Generic[T]):
    def __init__(
        self,
        key: str,
        validator: Type,
        default: Any = None,
        description: str = "",
        note: str = "",
        example: str = "",
    ):
        self.key = key.split(".")
        self.validator = validator
        self._value = None
        self.description = description
        self.note = note
        self.example = example
        if default is not None:
            self._value = self.validator.validate(default)

    @overload
    def __get__(self, instance: None, owner: Any) -> "Parameter[T]": ...

    @overload
    def __get__(self, instance: Any, owner: Any) -> T: ...

    def __get__(self, instance: Any, owner: Any) -> Any:
        return self

    def from_config(self, config: Config) -> T | None:
        x = config.dict
        for k in self.key:
            if k in x.keys():
                x = x[k]
            else:
                # The key is not defined in the config.
                break
        else:
            # At this point, the key was found and `x` is equal to its value.
            self._value = self.validator.validate(x)
        return self._value


class CustomParameter(Parameter):
    def __init__(self, *args, validator, **kwargs):
        kwargs["validator"] = CustomValidator(validator_fn=validator)
        super().__init__(*args, **kwargs)


class BoolParameter(Parameter[bool]):
    def __init__(self, *args, **kwargs):
        kwargs["validator"] = Bool()
        super().__init__(*args, **kwargs)


class IntParameter(Parameter[int]):
    def __init__(self, *args, **kwargs):
        kwargs["validator"] = Int()
        super().__init__(*args, **kwargs)


class FloatParameter(Parameter[float]):
    def __init__(self, *args, **kwargs):
        kwargs["validator"] = Float()
        super().__init__(*args, **kwargs)


class StringParameter(Parameter[str]):
    def __init__(self, *args, **kwargs):
        kwargs["validator"] = String()
        super().__init__(*args, **kwargs)


class TimeParameter(Parameter[time]):
    def __init__(self, *args, **kwargs):
        kwargs["validator"] = Time()
        super().__init__(*args, **kwargs)


class DurationParameter(Parameter[timedelta]):
    def __init__(self, *args, **kwargs):
        kwargs["validator"] = Duration()
        super().__init__(*args, **kwargs)


class EnumParameter(Parameter[Any]):
    def __init__(self, *args, values: list[Any] = [], **kwargs):
        kwargs["validator"] = Enum(values=values)
        super().__init__(*args, **kwargs)


class PathParameter(Parameter[Path]):
    def __init__(
        self,
        *args,
        check_file_exists: bool = False,
        check_dir_exists: bool = False,
        extensions: Optional[list[str]] = None,
        **kwargs,
    ):
        kwargs["validator"] = PathType(
            check_file_exists=check_file_exists,
            check_dir_exists=check_dir_exists,
            extensions=extensions,
        )
        super().__init__(*args, **kwargs)


class ListParameter(Parameter[list[Any]]):
    def __init__(
        self,
        *args,
        inner: Type,
        length: Optional[int] = None,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        **kwargs,
    ):
        kwargs["validator"] = List(inner, length, min_length, max_length)
        super().__init__(*args, **kwargs)
