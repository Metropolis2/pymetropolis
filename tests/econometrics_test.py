import numpy as np
import pandas as pd
import polars as pl
import pytest

from pymetropolis.metro_calibration.econometrics.mode_choice import (
    add_derived_variables,
    check_reference_vot,
    check_shared_car_driver_vot,
    estimate_mnl,
    monetary_cost_exprs,
)
from pymetropolis.metro_calibration.econometrics.preferences import (
    compute_mode_preferences,
    get_monetary_utility_scale,
    resolve_estimated_mode,
)
from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_pipeline.types import Dict, String
from pymetropolis.modes import (
    Bicycle,
    CarDriver,
    CarDriverWithPassengers,
    CarPassenger,
    CarRidesharing,
    MetaMode,
    PublicTransit,
    Walking,
)

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
    df = compute_mode_preferences(get_tours(), SPEC, PARAMS, PublicTransit)
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
    df = compute_mode_preferences(get_tours(), SPEC, PARAMS, Walking)
    assert df["walking_cst"].to_list() == [0.0, 0.0, 0.0]
    # Its value of time is estimated, though.
    assert df["walking_vot"].to_list() == pytest.approx([4.0, 6.0, 6.0])


def test_null_variables_are_ignored():
    """A null variable contributes zero, like a false / zero value: the third tour (null `woman`)
    gets the same preferences as the second one (`woman` is false), all else equal."""
    df = compute_mode_preferences(get_tours(), SPEC, PARAMS, PublicTransit)
    assert df["public_transit_cst"].to_list()[2] == pytest.approx(
        df["public_transit_cst"].to_list()[1]
    )
    assert df["public_transit_vot"].to_list()[2] == pytest.approx(
        df["public_transit_vot"].to_list()[1]
    )


def test_missing_variable_raises():
    tours = get_tours().drop("has_work_purpose")
    with pytest.raises(MetropyError):
        compute_mode_preferences(tours, SPEC, PARAMS, CarDriver)


def test_missing_variable_of_an_unused_coefficient_is_allowed():
    """`walking` is the reference mode of the estimated model, so the `woman` x `has_work_purpose`
    coefficient does not exist for it and the variables of that term are not needed."""
    tours = get_tours().drop("has_work_purpose")
    df = compute_mode_preferences(tours, SPEC, PARAMS, Walking)
    assert df["walking_cst"].to_list() == [0.0, 0.0, 0.0]


def test_quadratic_travel_time_raises():
    spec = {**SPEC, "interaction_variables": [["travel_time", "travel_time"]]}
    with pytest.raises(MetropyError):
        compute_mode_preferences(get_tours(), spec, PARAMS, Walking)


def test_car_modes_fall_back_to_the_grouped_car_driver_mode():
    assert resolve_estimated_mode(CarRidesharing, SPEC) == CarDriver
    car_driver = compute_mode_preferences(get_tours(), SPEC, PARAMS, CarDriver)
    ridesharing = compute_mode_preferences(get_tours(), SPEC, PARAMS, CarRidesharing)
    assert ridesharing["car_ridesharing_cst"].to_list() == car_driver["car_driver_cst"].to_list()
    assert ridesharing["car_ridesharing_vot"].to_list() == car_driver["car_driver_vot"].to_list()


def test_non_car_mode_without_coefficients_raises():
    with pytest.raises(MetropyError):
        resolve_estimated_mode(Bicycle, SPEC)


@pytest.mark.parametrize("shared", [False, True])
def test_monetary_cost_is_the_fuel_cost_paid_by_the_traveler(shared):
    """Car drivers pay the full fuel cost, car passengers pay nothing (unless the fuel cost is
    shared with the passengers), ridesharing travelers split the cost with the passengers and
    non-car modes have no monetary cost."""
    tours = pl.DataFrame({"distance_car": [10_000.0, None]})
    # 0.08 l/km * 2.0 €/l * 10 km = 1.6 €.
    exprs = monetary_cost_exprs(
        [CarDriver, CarDriverWithPassengers, CarPassenger, CarRidesharing, Walking],
        fuel_factor=0.08,
        fuel_price=2.0,
        fuel_share=lambda mode: mode.get_fuel_share(1.0, shared),
    )
    df = tours.select(**exprs)
    assert df["monetary_cost_car_driver"].to_list() == pytest.approx([1.6, None])
    assert df["monetary_cost_car_driver_with_passengers"].to_list() == pytest.approx(
        [0.8 if shared else 1.6, None]
    )
    assert df["monetary_cost_car_passenger"].to_list() == pytest.approx(
        [0.8 if shared else 0.0, None]
    )
    assert df["monetary_cost_car_ridesharing"].to_list() == pytest.approx([0.8, None])
    assert df["monetary_cost_walking"].to_list() == [0.0, 0.0]


@pytest.mark.parametrize(
    ("mode", "not_shared", "shared"),
    [
        (CarDriver, 1.0, 1.0),
        (CarDriverWithPassengers, 1.0, 1 / 3),
        (CarPassenger, 0.0, 1 / 3),
        (CarRidesharing, 1 / 3, 1 / 3),
    ],
)
def test_fuel_share(mode, not_shared, shared):
    assert mode.get_fuel_share(2.0) == pytest.approx(not_shared)
    assert mode.get_fuel_share(2.0, shared_with_passengers=True) == pytest.approx(shared)


def test_monetary_cost_is_not_a_constant_term():
    """The `monetary_cost` variable is alternative-varying: it does not enter the constant."""
    spec = {**SPEC, "variables": [*SPEC["variables"], "monetary_cost"]}
    params = {**PARAMS, "B_monetary_cost": {"value": -0.5}}
    expected = compute_mode_preferences(get_tours(), SPEC, PARAMS, PublicTransit)
    df = compute_mode_preferences(get_tours(), spec, params, PublicTransit)
    assert df.equals(expected)


def test_monetary_utility_scale():
    """The marginal utility of money is minus the monetary-cost coefficient."""
    assert get_monetary_utility_scale(PARAMS) is None
    assert get_monetary_utility_scale({**PARAMS, "B_monetary_cost": {"value": -0.5}}) == 0.5
    with pytest.raises(MetropyError):
        get_monetary_utility_scale({**PARAMS, "B_monetary_cost": {"value": 0.1}})


DERIVED = {
    "age_18_29": "age BETWEEN 18 AND 29",
    "age_65_plus": "age >= 65",
    "high_education": "education_level IN ('bachelor', 'master')",
}


def get_person_tours():
    return pl.DataFrame(
        {
            "tour_id": [1, 2, 3, 4],
            "age": [10, 25, 70, None],
            "education_level": ["primary", "master", "bachelor", None],
        }
    )


def test_derived_variables():
    """Derived variables are computed from SQL expressions and null inputs give null values."""
    df = add_derived_variables(get_person_tours(), DERIVED)
    assert df["age_18_29"].to_list() == [False, True, False, None]
    assert df["age_65_plus"].to_list() == [False, False, True, None]
    assert df["high_education"].to_list() == [False, True, True, None]


def test_derived_variables_cannot_shadow_columns():
    with pytest.raises(MetropyError, match="existing column"):
        add_derived_variables(get_person_tours(), {"age": "age + 1"})
    with pytest.raises(MetropyError, match="alternative-varying"):
        add_derived_variables(get_person_tours(), {"travel_time": "age"})


def test_invalid_derived_variable_raises():
    with pytest.raises(MetropyError, match="`bad`"):
        add_derived_variables(get_person_tours(), {"ok": "age >= 65", "bad": "unknown_col > 1"})
    with pytest.raises(MetropyError, match="`bad`"):
        add_derived_variables(get_person_tours(), {"bad": "age >>= 65"})


def test_derived_variables_in_preferences():
    """A derived dummy enters the constant like any other case variable."""
    spec = {
        "modes": ["car_driver", "walking"],
        "reference_mode": "walking",
        "variables": ["age_65_plus"],
        "interaction_variables": [],
        "derived_variables": {"age_65_plus": DERIVED["age_65_plus"]},
    }
    params = {"ASC_car_driver": {"value": 1.0}, "B_age_65_plus_car_driver": {"value": -2.0}}
    tours = add_derived_variables(get_person_tours(), spec["derived_variables"])
    df = compute_mode_preferences(tours, spec, params, CarDriver)
    # Age 65+: -(1.0 - 2.0). Otherwise (or null age): -1.0.
    assert df["car_driver_cst"].to_list() == pytest.approx([-1.0, -1.0, 1.0, -1.0])


def test_dict_type():
    validator = Dict(String())
    assert validator.validate({"a": "x"}) == {"a": "x"}
    with pytest.raises(MetropyError):
        validator.validate(["a"])
    with pytest.raises(MetropyError):
        validator.validate({1: "x"})
    with pytest.raises(MetropyError):
        validator.validate({"a": 1})


def simulate_choices(n: int = 2000) -> pd.DataFrame:
    """Simulates choices between `car_driver` (alt 0) and `walking` (alt 1, reference), with a
    utility of money of 0.5 /€ and a value of time of 15 €/h for car and 30 €/h for walking."""
    rng = np.random.default_rng(0)
    tt_car = rng.uniform(0.1, 1.0, n)
    tt_walk = rng.uniform(0.1, 1.5, n)
    cost_car = rng.uniform(0.0, 5.0, n)
    v_car = 0.5 - 0.5 * (15.0 * tt_car + cost_car)
    v_walk = -0.5 * 30.0 * tt_walk
    eps = rng.gumbel(size=(n, 2))
    choice = np.where(v_car + eps[:, 0] > v_walk + eps[:, 1], 0, 1)
    return pd.DataFrame(
        {
            "alt_id": choice,
            "weight": 1.0,
            "avail_car_driver": 1.0,
            "avail_walking": 1.0,
            "travel_time_car": tt_car,
            "travel_time_walking": tt_walk,
            "monetary_cost_car_driver": cost_car,
            "monetary_cost_walking": 0.0,
        }
    )


@pytest.mark.parametrize("with_cost", [True, False])
def test_estimation_with_reference_value_of_time(tmp_path, with_cost):
    """The `travel_time` coefficient of the reference mode is tied to the `monetary_cost`
    coefficient, so that the value of time of that mode is the requested one, whether or not the
    `monetary_cost` variable is part of the model."""
    generic_variables = ["travel_time", "monetary_cost"] if with_cost else ["travel_time"]
    generic_columns = {
        "travel_time": {"car_driver": "travel_time_car", "walking": "travel_time_walking"},
        "monetary_cost": {
            "car_driver": "monetary_cost_car_driver",
            "walking": "monetary_cost_walking",
        },
    }
    params, _ = estimate_mnl(
        simulate_choices(),
        {"car_driver": 0, "walking": 1},
        "walking",
        {v: generic_columns[v] for v in generic_variables},
        generic_variables,
        [],
        [],
        ("car_driver", 15.0),
        out_dir=tmp_path,
        model_name="test",
    )
    b_money = params["B_monetary_cost"]["value"]
    tt_car = params["B_travel_time_car_driver"]
    assert tt_car["derived"]
    assert tt_car["value"] == pytest.approx(15.0 * b_money)
    if with_cost:
        # The true marginal utility of money is identified by the cost variation.
        assert b_money == pytest.approx(-0.5, rel=0.2)

    spec = {
        "modes": ["car_driver", "walking"],
        "reference_mode": "walking",
        "variables": generic_variables,
        "interaction_variables": [],
    }
    scale = get_monetary_utility_scale(params)
    assert scale is not None
    df = compute_mode_preferences(pl.DataFrame({"tour_id": [1]}), spec, params, CarDriver)
    assert df["car_driver_vot"][0] / scale == pytest.approx(15.0)


def test_check_reference_vot():
    modes: list[MetaMode] = [CarDriver, Walking]
    assert check_reference_vot(None, None, ["travel_time"], modes) is None
    assert check_reference_vot(15.0, "car_driver", ["travel_time"], modes) == ("car_driver", 15.0)
    with pytest.raises(MetropyError):
        # Only one of the two parameters.
        check_reference_vot(15.0, None, ["travel_time"], modes)
    with pytest.raises(MetropyError):
        # No `travel_time` variable.
        check_reference_vot(15.0, "car_driver", ["monetary_cost"], modes)
    with pytest.raises(MetropyError):
        # Mode not estimated.
        check_reference_vot(15.0, "public_transit", ["travel_time"], modes)


def simulate_shared_vot_choices(n: int = 3000) -> pd.DataFrame:
    """Simulates choices between `car_driver` (alt 0), `car_driver_with_passengers` (alt 1) and
    `walking` (alt 2, reference), with a utility of money of 0.5 /€, a value of time of 15 €/h for
    both car modes and 30 €/h for walking, and distinct constants for the two car modes."""
    rng = np.random.default_rng(1)
    tt_car = rng.uniform(0.1, 1.0, n)
    tt_cdwp = rng.uniform(0.1, 1.0, n)
    tt_walk = rng.uniform(0.1, 1.5, n)
    cost_car = rng.uniform(0.0, 5.0, n)
    cost_cdwp = rng.uniform(0.0, 5.0, n)
    v_car = 0.5 - 0.5 * (15.0 * tt_car + cost_car)
    v_cdwp = -1.0 - 0.5 * (15.0 * tt_cdwp + cost_cdwp)
    v_walk = -0.5 * 30.0 * tt_walk
    eps = rng.gumbel(size=(n, 3))
    choice = np.argmax(np.column_stack([v_car, v_cdwp, v_walk]) + eps, axis=1)
    return pd.DataFrame(
        {
            "alt_id": choice,
            "weight": 1.0,
            "avail_car_driver": 1.0,
            "avail_car_driver_with_passengers": 1.0,
            "avail_walking": 1.0,
            "travel_time_car_driver": tt_car,
            "travel_time_car_driver_with_passengers": tt_cdwp,
            "travel_time_walking": tt_walk,
            "monetary_cost_car_driver": cost_car,
            "monetary_cost_car_driver_with_passengers": cost_cdwp,
            "monetary_cost_walking": 0.0,
        }
    )


@pytest.mark.parametrize("with_reference_vot", [False, True])
def test_estimation_with_shared_car_driver_vot(tmp_path, with_reference_vot):
    """`car_driver` and `car_driver_with_passengers` share a single `travel_time` coefficient,
    reported under both names, while keeping their own constants."""
    modes = ["car_driver", "car_driver_with_passengers", "walking"]
    generic_variables = ["travel_time", "monetary_cost"]
    generic_columns = {v: {m: f"{v}_{m}" for m in modes} for v in generic_variables}
    reference_vot = ("car_driver", 15.0) if with_reference_vot else None
    params, _ = estimate_mnl(
        simulate_shared_vot_choices(),
        {m: i for i, m in enumerate(modes)},
        "walking",
        generic_columns,
        generic_variables,
        [],
        [],
        reference_vot,
        {"car_driver_with_passengers": "car_driver"},
        out_dir=tmp_path,
        model_name="test",
    )
    tt_car = params["B_travel_time_car_driver"]
    tt_cdwp = params["B_travel_time_car_driver_with_passengers"]
    assert tt_cdwp["value"] == tt_car["value"]
    assert tt_cdwp["derived"]
    assert tt_cdwp["shared_with"] == "car_driver"
    assert params["ASC_car_driver"]["value"] != params["ASC_car_driver_with_passengers"]["value"]
    assert params["ASC_car_driver"]["value"] == pytest.approx(0.5, abs=0.3)
    assert params["ASC_car_driver_with_passengers"]["value"] == pytest.approx(-1.0, abs=0.3)

    scale = get_monetary_utility_scale(params)
    assert scale is not None
    if with_reference_vot:
        assert tt_car["value"] == pytest.approx(15.0 * params["B_monetary_cost"]["value"])
        spec = {
            "modes": modes,
            "reference_mode": "walking",
            "variables": generic_variables,
            "interaction_variables": [],
        }
        df = compute_mode_preferences(
            pl.DataFrame({"tour_id": [1]}), spec, params, CarDriverWithPassengers
        )
        assert df["car_driver_with_passengers_vot"][0] / scale == pytest.approx(15.0)
    else:
        assert -tt_car["value"] / scale == pytest.approx(15.0, rel=0.2)


def test_check_shared_car_driver_vot():
    modes: list[MetaMode] = [CarDriver, CarDriverWithPassengers, CarPassenger, Walking]
    assert check_shared_car_driver_vot(False, [Walking], [], None) == {}
    # Only the car-driver modes are shared (not `car_passenger`).
    assert check_shared_car_driver_vot(True, modes, ["travel_time"], None) == {
        "car_driver_with_passengers": "car_driver"
    }
    # The reference value-of-time mode is the source of the shared coefficients.
    assert check_shared_car_driver_vot(
        True, modes, ["travel_time"], ("car_driver_with_passengers", 15.0)
    ) == {"car_driver": "car_driver_with_passengers"}
    assert check_shared_car_driver_vot(True, modes, ["travel_time"], ("walking", 15.0)) == {
        "car_driver_with_passengers": "car_driver"
    }
    with pytest.raises(MetropyError):
        check_shared_car_driver_vot(
            True, [CarDriver, CarPassenger, CarRidesharing, Walking], ["travel_time"], None
        )
    with pytest.raises(MetropyError):
        check_shared_car_driver_vot(True, modes, ["monetary_cost"], None)
