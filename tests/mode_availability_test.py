import tempfile

import polars as pl
from loguru import logger

from pymetropolis.metro_pipeline import Config
from pymetropolis.modes import (
    Bicycle,
    CarDriver,
    CarDriverWithPassengers,
    CarPassenger,
    CarRidesharing,
    ModeAvailabilityRules,
    PublicTransit,
    StepWithModeAvailability,
    Walking,
    mode_availability_expr,
    warn_missing_availability_columns,
)

PERSONS = pl.DataFrame(
    {
        "nb_cars": [1, 0, 1, None, 1, 1],
        "has_driving_license": [True, True, False, True, None, True],
        "age": [30, 18, 40, 17, None, 25],
        "joint_tour": [False, False, False, False, False, True],
        "nb_bicycles": [0, 1, None, 2, 0, 1],
        "total_distance": [1000.0, 6000.0, 20000.0, None, 4000.0, 12000.0],
    }
)


def available(mode, rules: ModeAvailabilityRules, df: pl.DataFrame = PERSONS) -> list[bool]:
    expr = mode_availability_expr(mode, rules, df.columns).alias("avail")
    return df.with_columns(expr)["avail"].to_list()


def test_default_rules():
    rules = ModeAvailabilityRules()
    # Car owners with a driving license, on non-joint tours (null values are unavailable).
    assert available(CarDriver, rules) == [True, False, False, False, False, False]
    # Joint tours do not restrict modes with passengers.
    assert available(CarDriverWithPassengers, rules) == [True, False, False, False, False, True]
    # Car owners.
    assert available(CarPassenger, rules) == [True, False, True, False, True, True]
    assert available(CarRidesharing, rules) == [True, False, True, False, True, True]
    # No restriction for the other modes.
    for mode in (PublicTransit, Walking, Bicycle):
        assert all(available(mode, rules))


def test_car_driver_without_car_ownership_but_min_age():
    rules = ModeAvailabilityRules(car_driver_requires_car=False, car_driver_min_age=18)
    # Person 1 (aged 18, no car) is now available; person 3 (aged 17) and person 4 (null age) are
    # not.
    assert available(CarDriver, rules) == [True, True, False, False, False, False]
    assert available(CarDriverWithPassengers, rules) == [True, True, False, False, False, True]
    # Car-passenger rules are independent.
    assert available(CarPassenger, rules) == [True, False, True, False, True, True]


def test_relaxed_car_rules():
    rules = ModeAvailabilityRules(
        car_driver_requires_car=False,
        car_driver_requires_driving_license=False,
        car_passenger_requires_car=False,
        car_ridesharing_requires_car=False,
        no_solo_modes_for_joint_tours=False,
    )
    for mode in (CarDriver, CarDriverWithPassengers, CarPassenger, CarRidesharing):
        assert all(available(mode, rules))


def test_bicycle_and_distance_rules():
    rules = ModeAvailabilityRules(
        bicycle_requires_bicycle=True, walking_max_distance=5000, bicycle_max_distance=15000
    )
    # Null distances do not restrict availability.
    assert available(Walking, rules) == [True, False, False, True, True, False]
    assert available(Bicycle, rules) == [False, True, False, True, False, True]


def test_missing_columns_are_skipped():
    rules = ModeAvailabilityRules(car_driver_min_age=18)
    df = PERSONS.select("has_driving_license")
    assert available(CarDriver, rules, df) == [True, True, False, True, False, True]
    df = pl.DataFrame({"trip_id": [1, 2]})
    assert available(CarDriver, rules, df) == [True, True]


def test_missing_column_warnings():
    messages: list[str] = []
    handler = logger.add(messages.append, level="WARNING", format="{message}")
    try:
        rules = ModeAvailabilityRules(car_driver_min_age=18)
        warn_missing_availability_columns(
            [CarDriver, PublicTransit, Walking], rules, ["nb_cars", "has_driving_license"]
        )
    finally:
        logger.remove(handler)
    # Only the age and joint-tour rules of `car_driver` are active and missing.
    assert len(messages) == 2
    assert any("`age`" in m for m in messages)
    assert any("`joint_tour`" in m for m in messages)


class AvailabilityStep(StepWithModeAvailability):
    pass


def test_rules_from_config():
    with tempfile.TemporaryDirectory() as tmp_dir:
        step = AvailabilityStep(Config({"main_directory": tmp_dir}))
        assert step.availability_rules() == ModeAvailabilityRules()
        assert not step.has_distance_caps()

        config = Config(
            {
                "main_directory": tmp_dir,
                "mode_availability": {
                    "car_driver": {"requires_car": False, "min_age": 18},
                    "car_passenger": {"requires_car": False},
                    "no_solo_modes_for_joint_tours": False,
                    "walking": {"max_distance": 5000.0},
                },
            }
        )
        step = AvailabilityStep(config)
        assert step.availability_rules() == ModeAvailabilityRules(
            car_driver_requires_car=False,
            car_driver_min_age=18,
            car_passenger_requires_car=False,
            no_solo_modes_for_joint_tours=False,
            walking_max_distance=5000.0,
        )
        assert step.has_distance_caps()
