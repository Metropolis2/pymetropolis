from pymetropolis.metro_pipeline.parameters import FloatParameter, ListParameter
from pymetropolis.metro_pipeline.steps import Step
from pymetropolis.metro_pipeline.types import Enum


class StepWithModes(Step):
    modes = ListParameter(
        "mode_choice.modes",
        inner=Enum(
            values=[
                "car_driver",
                "car_driver_with_passengers",
                "car_passenger",
                "car_ridesharing",
                "public_transit",
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
        return self.modes is not None and any(map(lambda m: m != "outside_option", self.modes))

    def has_car_mode(self) -> bool:
        """Returns `True` if the configuration has at least one car-based mode."""
        return self.modes is not None and (
            "car_driver" in self.modes
            or "car_driver_with_passengers" in self.modes
            or "car_passenger" in self.modes
            or "car_ridesharing" in self.modes
        )


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
