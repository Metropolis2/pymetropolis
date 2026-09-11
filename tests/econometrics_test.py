import polars as pl
import pytest

from pymetropolis.metro_calibration.econometrics.preferences import (
    compute_mode_preferences,
    get_utility_scale,
    resolve_estimated_mode,
)
from pymetropolis.metro_common import MetropyError

# A model estimated on 3 alternatives, with `walking` as the reference mode, one case variable
# (`woman`), one alternative-varying variable (`travel_time`), one alternative-varying interaction
# (`woman` x `travel_time`) and one case-varying interaction (`woman` x `has_work_purpose`, whose
# `has_work_purpose` variable is used in the interaction only).
SPEC = {
    "modes": ["car_driver", "public_transit", "walking"],
    "reference_mode": "walking",
    "variables": ["woman", "travel_time"],
    "interaction_variables": [["woman", "travel_time"], ["woman", "has_work_purpose"]],
}

PARAMS = {
    "ASC_car_driver": {"value": 2.0},
    "ASC_public_transit": {"value": 1.0},
    "B_travel_time_car_driver": {"value": -4.0},
    "B_travel_time_public_transit": {"value": -3.0},
    "B_travel_time_walking": {"value": -6.0},
    "B_woman_car_driver": {"value": -0.5},
    "B_woman_public_transit": {"value": 0.25},
    "B_woman_x_travel_time_car_driver": {"value": -1.0},
    "B_woman_x_travel_time_public_transit": {"value": 0.5},
    "B_woman_x_travel_time_walking": {"value": 2.0},
    "B_woman_x_has_work_purpose_car_driver": {"value": 0.75},
    "B_woman_x_has_work_purpose_public_transit": {"value": -0.6},
}


def get_tours():
    """Three tours: a woman, a man and a person whose gender is unknown."""
    return pl.DataFrame(
        {"tour_id": [1, 2, 3], "woman": [True, False, None], "has_work_purpose": [True, True, True]}
    )


def test_constant_and_value_of_time():
    """The preferences are the opposite of the estimated utilities, in utils."""
    df = compute_mode_preferences(get_tours(), SPEC, PARAMS, "public_transit")
    assert df["tour_id"].to_list() == [1, 2, 3]
    # Woman, with a work purpose: 1.0 + 0.25 - 0.6.
    # Man, with a work purpose: 1.0.
    # Null `woman` (counted as zero), with a work purpose: 1.0.
    assert df["public_transit_cst"].to_list() == pytest.approx([-0.65, -1.0, -1.0])
    # Woman: -3.0 + 0.5. Man and null `woman`: -3.0.
    assert df["public_transit_vot"].to_list() == pytest.approx([2.5, 3.0, 3.0])


def test_estimation_reference_mode_has_a_zero_constant():
    """The reference mode of the estimated model has no constant and no case-variable
    coefficient, so its constant is zero for every tour."""
    df = compute_mode_preferences(get_tours(), SPEC, PARAMS, "walking")
    assert df["walking_cst"].to_list() == [0.0, 0.0, 0.0]
    # Its value of time is estimated, though.
    assert df["walking_vot"].to_list() == pytest.approx([4.0, 6.0, 6.0])


def test_null_variables_are_ignored():
    """A null variable contributes zero, like a false / zero value: the third tour (null `woman`)
    gets the same preferences as the second one (`woman` is false), all else equal."""
    df = compute_mode_preferences(get_tours(), SPEC, PARAMS, "public_transit")
    assert df["public_transit_cst"].to_list()[2] == pytest.approx(
        df["public_transit_cst"].to_list()[1]
    )
    assert df["public_transit_vot"].to_list()[2] == pytest.approx(
        df["public_transit_vot"].to_list()[1]
    )


def test_missing_variable_raises():
    tours = get_tours().drop("has_work_purpose")
    with pytest.raises(MetropyError):
        compute_mode_preferences(tours, SPEC, PARAMS, "car_driver")


def test_missing_variable_of_an_unused_coefficient_is_allowed():
    """`walking` is the reference mode of the estimated model, so the `woman` x `has_work_purpose`
    coefficient does not exist for it and the variables of that term are not needed."""
    tours = get_tours().drop("has_work_purpose")
    df = compute_mode_preferences(tours, SPEC, PARAMS, "walking")
    assert df["walking_cst"].to_list() == [0.0, 0.0, 0.0]


def test_quadratic_travel_time_raises():
    spec = {**SPEC, "interaction_variables": [["travel_time", "travel_time"]]}
    with pytest.raises(MetropyError):
        compute_mode_preferences(get_tours(), spec, PARAMS, "walking")


def test_car_modes_fall_back_to_the_grouped_car_driver_mode():
    assert resolve_estimated_mode("car_ridesharing", SPEC) == "car_driver"
    car_driver = compute_mode_preferences(get_tours(), SPEC, PARAMS, "car_driver")
    ridesharing = compute_mode_preferences(get_tours(), SPEC, PARAMS, "car_ridesharing")
    assert ridesharing["car_ridesharing_cst"].to_list() == car_driver["car_driver_cst"].to_list()
    assert ridesharing["car_ridesharing_vot"].to_list() == car_driver["car_driver_vot"].to_list()


def test_non_car_mode_without_coefficients_raises():
    with pytest.raises(MetropyError):
        resolve_estimated_mode("bicycle", SPEC)


def test_utility_scale_matches_the_average_value_of_time():
    """Dividing by the scale turns the utils into euros, such that the *average* value of time of
    the reference mode in the population is the requested one."""
    reference_vot = 20.0
    df = compute_mode_preferences(get_tours(), SPEC, PARAMS, "car_driver")
    # Value of time of each tour, in utils per hour: 5.0 for the woman, 4.0 for the two others.
    assert df["car_driver_vot"].to_list() == pytest.approx([5.0, 4.0, 4.0])

    scale = get_utility_scale(df["car_driver_vot"], reference_vot, "car_driver")
    assert scale == pytest.approx((5.0 + 4.0 + 4.0) / 3 / reference_vot)

    euros = df.with_columns(pl.col("car_driver_cst", "car_driver_vot") / scale)
    assert euros["car_driver_vot"].mean() == pytest.approx(reference_vot)
    # The value of time of the woman is above the average, the one of the two others below.
    assert euros["car_driver_vot"].to_list() == pytest.approx(
        [5.0 / scale, 4.0 / scale, 4.0 / scale]
    )
    assert euros["car_driver_cst"].to_list() == pytest.approx(
        [-2.25 / scale, -2.0 / scale, -2.0 / scale]
    )


def test_utility_scale_requires_a_positive_average_value_of_time():
    """A travel-time coefficient that is not negative (i.e., a value of time that is not positive)
    cannot be used to convert the utilities to euros."""
    params = {**PARAMS, "B_travel_time_car_driver": {"value": 1.0}}
    df = compute_mode_preferences(get_tours(), SPEC, params, "car_driver")
    with pytest.raises(MetropyError):
        get_utility_scale(df["car_driver_vot"], 20.0, "car_driver")
