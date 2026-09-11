from pymetropolis.metro_pipeline.parameters import ListParameter
from pymetropolis.metro_pipeline.steps import Step
from pymetropolis.metro_pipeline.types import Enum

# TODO: Create Mode class.


class StepWithModes(Step):
    """A Step subclass for Steps that depend on the list of simulated modes.

    The class is defined here, and not in `metro_simulation`, because it is used by Steps of the
    demand, the simulation and the calibration packages.
    """

    modes = ListParameter(
        "mode_choice.modes",
        inner=Enum(
            values=[
                "car_driver",
                "car_driver_with_passengers",
                "car_passenger",
                "car_ridesharing",
                "public_transit",
                "walking",
                "bicycle",
                "outside_option",
            ]
        ),
        min_length=1,
        description="List of modes the agents can used to travel.",
    )

    def has_mode_choice(self) -> bool:
        """Returns `True` if the configuration implies a mode choice (i.e., there are at least two
        modes).
        """
        return self.modes is not None and len(self.modes) >= 2

    def has_mode(self, mode: str) -> bool:
        """Returns `True` if the configuration has a given mode defined."""
        return self.modes is not None and mode in self.modes

    def has_trip_mode(self) -> bool:
        """Returns `True` if the configuration has at least one trip-based mode (i.e., different
        from "outside_option").
        """
        return self.modes is not None and any(m != "outside_option" for m in self.modes)

    def has_car_mode(self) -> bool:
        """Returns `True` if the configuration has at least one car-based mode."""
        return self.modes is not None and (
            "car_driver" in self.modes
            or "car_driver_with_passengers" in self.modes
            or "car_passenger" in self.modes
            or "car_ridesharing" in self.modes
        )

    def has_pedestrian_mode(self) -> bool:
        """Returns `True` if the configuration has at least one pedestrian mode (e.g., walking,
        bicycle).
        """
        return self.modes is not None and ("walking" in self.modes or "bicycle" in self.modes)
