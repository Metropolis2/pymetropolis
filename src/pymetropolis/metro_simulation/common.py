from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_pipeline.parameters import FloatParameter, FractionParameter
from pymetropolis.metro_pipeline.steps import Step

if TYPE_CHECKING:
    from collections.abc import Mapping

    import polars as pl

    from pymetropolis.metro_pipeline import MetroFile


def merge_populations(
    files: Mapping[str, MetroFile], id_columns: tuple[str, ...] = ("agent_id",)
) -> pl.DataFrame:
    """Reads a MetroDataFrameFile for each population and concatenates them vertically.

    Each column in `id_columns` is prefixed with `f"{population}-"` so that ids stay globally
    unique after the merge (a plain agent_id / trip_id is only unique within its own population's
    file).
    """
    import polars as pl

    dfs = [
        f.read().with_columns(
            **{col: pl.concat_str(pl.lit(f"{population}-"), pl.col(col)) for col in id_columns}
        )
        for population, f in files.items()
    ]
    return pl.concat(dfs, how="vertical")


# Ridesharing passenger count is used for both vehicle types and trips so we create a Step for it.
class StepWithRidesharingCount(Step):
    ridesharing_passenger_count = FloatParameter(
        "vehicle_types.car.ridesharing_passenger_count",
        default=1.0,
        description="Average number of passengers in the car (excluding the driver).",
        note=(
            "This is only relevant for the `car_ridesharing` mode. "
            "Larger values increase probability to select this mode (fuel cost is shared between "
            "more persons) and decrease congestion generated (more persons are traveling in each "
            "car)."
        ),
    )


class StepWithSimulationRatio(Step):
    simulation_ratio = FractionParameter(
        "simulation_ratio",
        default=1.0,
        description="Ratio of the population that is being simulated.",
        note=(
            "This value controls how road capacities and aggregate results are scaled when the "
            "simulated agents do not represent 100% of population. "
            "It does _not_ affect the scaling of the input origin-destination matrix or synthetic "
            "population."
        ),
    )
