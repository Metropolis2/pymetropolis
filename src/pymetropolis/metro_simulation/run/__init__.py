from __future__ import annotations

from typing import Any

from .files import (
    MetroAgentResultsFile,
    MetroExpectedTravelTimeFunctionsFile,
    MetroIterationResultsFile,
    MetroNextExpectedTravelTimeFunctionsFile,
    MetroSimulatedTravelTimeFunctionsFile,
    MetroTripResultsFile,
)

RUN_FILES = [
    MetroIterationResultsFile,
    MetroTripResultsFile,
    MetroAgentResultsFile,
    MetroSimulatedTravelTimeFunctionsFile,
    MetroExpectedTravelTimeFunctionsFile,
    MetroNextExpectedTravelTimeFunctionsFile,
]

_LAZY_ATTRS = ("RUN_STEPS",)


def __getattr__(name: str) -> Any:
    """Lazily imports RunSimulationStep.

    Deferred because '.exec' imports 'metro_simulation.parameters', which imports
    'metro_simulation.demand', which imports back from 'metro_demand.departure_time' -
    a cycle that closes if this runs eagerly while 'metro_demand' (e.g.
    'metro_demand.routing.od_zones') is itself mid-import reaching for
    'metro_simulation.run.files'.
    """
    if name not in _LAZY_ATTRS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    from .exec import RunSimulationStep

    steps = [RunSimulationStep]
    globals()["RUN_STEPS"] = steps
    return steps
