import os
from datetime import time, timedelta
from typing import Any, Optional

from isodate import ISO8601Error, parse_duration

from .errors import MetropyError


class MetroType:
    def check(self, value: Any):
        return True

    def parse(self, value: Any):
        return value


class Duration(MetroType):
    def check(self, value: Any):
        if isinstance(value, float | int):
            return True
        if isinstance(value, str):
            try:
                parse_duration(value)
            except ISO8601Error:
                return False
            else:
                return True
        return False

    def parse(self, value: Any) -> timedelta:
        if isinstance(value, float | int):
            return timedelta(seconds=value)
        if isinstance(value, str):
            try:
                return parse_duration(value)
            except ISO8601Error:
                pass
        raise MetropyError("Cannot parse duration")


class Time(MetroType):
    def check(self, value: Any):
        if isinstance(value, time):
            return True
        if isinstance(value, str):
            try:
                time.fromisoformat(value)
            except ValueError:
                return False
            else:
                return True
        return False

    def parse(self, value: Any) -> time:
        if isinstance(value, time):
            return value
        if isinstance(value, str):
            try:
                return time.fromisoformat(value)
            except ValueError:
                pass
        raise MetropyError("Cannot parse time")


class FixedSizeList(MetroType):
    def __init__(self, length: int, inner_type: Any = None):
        self.length = length
        self.inner_type = inner_type

    def check(self, value: Any):
        if not isinstance(value, list):
            return False
        if len(value) != self.length:
            return False
        if isinstance(self.inner_type, MetroType):
            return all(self.inner_type.check(v) for v in value)
        elif self.inner_type is not None:
            return all(isinstance(v, self.inner_type) for v in value)
        else:
            raise MetropyError(f"Invalid inner type fro FixedSizeList: `{self.inner_type}`")


class Path(MetroType):
    def __init__(self, extensions: Optional[list[str]] = None):
        if extensions:
            self.extensions = set(extensions)
        else:
            self.extensions = None

    def check(self, value: Any):
        try:
            os.path.isfile(value)
        except TypeError:
            return False
        else:
            if self.extensions:
                # Further check that the extension is valid.
                _, ext = os.path.splitext(value)
                return ext in self.extensions
            else:
                return True


class FixedValues(MetroType):
    def __init__(self, possible_values: list[Any]):
        self.possible_values = set(possible_values)

    def check(self, value: Any):
        return value in self.possible_values
