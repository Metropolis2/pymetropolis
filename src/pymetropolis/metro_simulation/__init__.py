from __future__ import annotations

from typing import Any

_LAZY_ATTRS = ("FILES", "STEPS")


def __getattr__(name: str) -> Any:
    """Lazily builds FILES/STEPS on first access.

    Deferred so that importing a single submodule (e.g. 'metro_simulation.run.files')
    does not eagerly pull in '.demand', which itself imports from 'metro_demand'.
    a package that can, transtively, import back from 'metro_simulation' (e.g.
    'metro_demand.routing.od_zones'). Eagerly running the two imports below at
    package-load time closes that cycle and breaks 'import pymetropolis'.
    """
    if name not in _LAZY_ATTRS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    from .demand import DEMAND_FILES, DEMAND_STEPS
    from .parameters import PARAMETERS_FILES, PARAMETERS_STEPS
    from .run import RUN_FILES, RUN_STEPS
    from .supply import SUPPLY_FILES, SUPPLY_STEPS

    files = DEMAND_FILES + PARAMETERS_FILES + SUPPLY_FILES + RUN_FILES
    steps = DEMAND_STEPS + PARAMETERS_STEPS + SUPPLY_STEPS + RUN_STEPS
    globals()["FILES"] = files
    globals()["STEPS"] = steps
    return files if name == "FILES" else steps
