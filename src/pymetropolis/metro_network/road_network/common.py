from typing import Any

from pymetropolis.metro_common import MetropyError


def is_valid_map(value: dict) -> bool:
    """Returns True if the given value is a valid map key->numeric value."""
    return all(isinstance(v, int | float) for v in value.values())


def default_edge_values_validator(value: Any) -> int | float | dict:
    """A validator function for a parameter that defines default values for edges.

    Returns the value if it is a valid paramete input, otherwise raises an error.

    Three cases:
        - The same value (numeric) for all edges.
        - A map road_type -> numeric value.
        - A nested map urban/rural -> road_type -> numeric value.
    """
    if isinstance(value, int | float):
        # Case 1. constant penalty
        return value
    elif isinstance(value, dict):
        keys = set(value.keys())
        if keys == {"urban", "rural"}:
            # Case 2. nested map urban/rural -> road_type -> penalty
            for k in keys:
                if not is_valid_map(value[k]):
                    raise MetropyError(
                        f"Invalid {k} penalties (map road_type->penalty expected): `{value[k]}`"
                    )
            else:
                return value
        else:
            # Case 3. map road_type -> penalty
            if is_valid_map(value):
                return value
            else:
                raise MetropyError(
                    f"Invalid penalties (map road_type->penalty expected): `{value}`"
                )
    else:
        raise MetropyError(f"Invalid penalties (number or dictionary expected): `{value}`")
