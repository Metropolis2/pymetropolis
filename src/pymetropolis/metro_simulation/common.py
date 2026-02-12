from pymetropolis.metro_pipeline.parameters import ListParameter
from pymetropolis.metro_pipeline.steps import Step
from pymetropolis.metro_pipeline.types import Enum


class StepWithModes(Step):
    modes = ListParameter(
        "mode_choice.modes",
        inner=Enum(values=["car_driver", "public_transit", "outside_option"]),
        min_length=1,
        description="List of modes the agents can used to travel.",
    )

    def has_trip_modes(self) -> bool:
        """Returns `True` if the configuration has at least one trip-based mode (i.e., different
        from "outside_option").
        """
        return self.modes is not None and any(map(lambda m: m != "outside_option", self.modes))

    def has_mode_choice(self) -> bool:
        """Returns `True` if the configuration implies a mode choice (i.e., there are at least two
        modes).
        """
        return self.modes is not None and len(self.modes) >= 2

    def has_mode(self, mode: str) -> bool:
        """Returns `True` if the configuration has a given mode defined."""
        return self.modes is not None and mode in self.modes
