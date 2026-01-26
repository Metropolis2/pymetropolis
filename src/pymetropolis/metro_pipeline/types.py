from datetime import time, timedelta
from pathlib import Path
from typing import Any, Optional, override

from isodate import ISO8601Error, parse_duration

from pymetropolis.metro_common.errors import MetropyError


class Type:
    def validate(self, value: Any) -> Any:
        return value


class CustomValidator:
    def __init__(self, validator_fn):
        self.validator_fn = validator_fn

    def validate(self, value: Any) -> Any:
        return self.validator_fn(value)


class Int(Type):
    @override
    def validate(self, value: Any) -> int:
        if not isinstance(value, int):
            raise MetropyError(f"Invalid integer: {value}")
        return value


class PathType(Type):
    check_file_exists: bool
    check_dir_exists: bool
    extensions: Optional[list[str]]

    def __init__(
        self,
        check_file_exists: bool = False,
        check_dir_exists: bool = False,
        extensions: Optional[list[str]] = None,
    ):
        self.check_file_exists = check_file_exists
        self.check_dir_exists = check_dir_exists
        self.extensions = extensions

    @override
    def validate(self, value: Any) -> Path:
        if isinstance(value, str):
            value = Path(value)
        if not isinstance(value, Path):
            raise MetropyError(f"Invalid path: {value}")
        if self.extensions is not None and value.suffix not in self.extensions:
            ext_str = ", ".join(self.extensions)
            raise MetropyError(f"Invalid path (allowed extensions: {ext_str}): {value}")
        if self.check_file_exists and not value.is_file():
            raise MetropyError(f"Invalid path (not a file): {value}")
        if self.check_dir_exists and not value.is_dir():
            raise MetropyError(f"Invalid path (not a directory): {value}")
        return value


class Enum(Type):
    values: set[Any]

    def __init__(self, values: list[Any] = []):
        self.values = set(values)

    @override
    def validate(self, value: Any) -> Any:
        if value not in self.values:
            values_str = ", ".join(map(repr, self.values))
            raise MetropyError(f"Invalid value: {value} [Expected one of: {values_str}]")
        return value


class Bool(Type):
    @override
    def validate(self, value: Any) -> bool:
        if not isinstance(value, bool):
            raise MetropyError(f"Invalid boolean: {value}")
        return value


class Float(Type):
    @override
    def validate(self, value: Any) -> float:
        if not isinstance(value, int | float):
            raise MetropyError(f"Invalid float: {value}")
        return float(value)


class String(Type):
    @override
    def validate(self, value: Any) -> str:
        if not isinstance(value, str):
            raise MetropyError(f"Invalid string: {repr(value)}")
        return value


class Duration(Type):
    @override
    def validate(self, value: Any) -> timedelta:
        if isinstance(value, timedelta):
            return value
        if isinstance(value, float | int) and value >= 0:
            return timedelta(seconds=value)
        if isinstance(value, str):
            try:
                return parse_duration(value)
            except ISO8601Error:
                pass
        raise MetropyError(f"Invalid duration: {value}")


class Time(Type):
    @override
    def validate(self, value: Any) -> time:
        if isinstance(value, time):
            return value
        if isinstance(value, str):
            try:
                return time.fromisoformat(value)
            except ValueError:
                pass
        raise MetropyError(f"Invalid time: {value}")


class List(Type):
    inner: Type
    length: Optional[int]
    min_length: Optional[int]
    max_length: Optional[int]

    def __init__(
        self,
        inner: Type,
        length: Optional[int] = None,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
    ):
        self.inner = inner
        self.length = length
        self.min_length = min_length
        self.max_length = max_length

    def validate(self, value: Any) -> list[Any]:
        if not isinstance(value, list):
            raise MetropyError(f"Invalid list: {value}")
        if self.length is not None and len(value) != self.length:
            raise MetropyError(
                f"List has invalid number of elements (found: {len(value)}, expected: {self.length}): {value}"
            )
        if self.min_length is not None and len(value) < self.min_length:
            raise MetropyError(
                f"List has not enough elements (found: {len(value)}, expected: {self.length}+): {value}"
            )
        if self.max_length is not None and len(value) > self.max_length:
            raise MetropyError(
                f"List has too many elements (found: {len(value)}, expected: {self.length}-): {value}"
            )
        res = list()
        for elem in value:
            res.append(self.inner.validate(elem))
        return res
