import re
from datetime import time
from functools import total_ordering

from pymetropolis.metro_common import MetropyError


@total_ordering
class MetroTime:
    """A special Time variable for Metro operations that can overflow over 24 hours."""

    _seconds_since_midnight: float

    def __init__(self, value: float):
        self._seconds_since_midnight = value

    def __str__(self):
        hours = int(self._seconds_since_midnight // 3600)
        remaining_seconds = self._seconds_since_midnight % 3600
        minutes = int(remaining_seconds // 60)
        seconds = remaining_seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:06.3f}"

    def __hash__(self):
        return hash(self._seconds_since_midnight)

    def __eq__(self, other) -> bool:
        return self._seconds_since_midnight == other._seconds_since_midnight

    def __lt__(self, other: "MetroTime") -> bool:
        return self._seconds_since_midnight < other._seconds_since_midnight

    def from_seconds(seconds: float) -> "MetroTime":
        return MetroTime(seconds)

    def from_time(t: time) -> "MetroTime":
        return MetroTime(t.hour * 3600 + t.minute * 60 + t.second + t.microsecond / 1e6)

    def from_str(value: str) -> "MetroTime":
        pattern = r"^(\d+):(\d{1,2})(?::(\d{1,2}(?:\.\d+)?))?$"
        match = re.match(pattern, value)
        if not match:
            raise MetropyError(f"Invalid time string format: {value}")
        h = int(match.group(1))
        m = int(match.group(2))
        s = float(match.group(3)) if match.group(3) else 0.0
        return MetroTime(h * 3600 + m * 60 + s)

    def parse(value) -> "MetroTime":
        if isinstance(value, float | int):
            return MetroTime.from_seconds(value)
        if isinstance(value, time):
            return MetroTime.from_time(value)
        if isinstance(value, str):
            return MetroTime.from_str(value)
        raise MetropyError(f"Invalid time: {value}")

    def seconds(self) -> float:
        return self._seconds_since_midnight
