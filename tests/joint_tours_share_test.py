import polars as pl
import pytest

from pymetropolis.metro_calibration.survey.joint_travel import compute_joint_tours_shares
from pymetropolis.metro_common.plots import plot_joint_share_comparison


def test_compute_joint_tours_shares():
    survey = pl.DataFrame(
        {
            "joint_tour": [True, False, False],
            "weight": [1.0, 2.0, 1.0],
            "weighted_distance": [100.0, 600.0, 300.0],
        }
    )
    sim = pl.DataFrame(
        {"joint_tour": [True, True, False, False], "total_distance": [10.0, 30.0, 40.0, 20.0]}
    )
    res = compute_joint_tours_shares(survey, sim)
    assert res["tour_count"]["survey"] == pytest.approx(0.25)
    assert res["tour_count"]["simulation"] == pytest.approx(0.5)
    assert res["distance"]["survey"] == pytest.approx(0.1)
    assert res["distance"]["simulation"] == pytest.approx(0.4)
    assert res["totals"]["survey_weight"] == pytest.approx(4.0)
    assert res["totals"]["simulation_tours"] == 4


def test_compute_joint_tours_shares_no_distance():
    survey = pl.DataFrame({"joint_tour": [True], "weight": [1.0], "weighted_distance": [100.0]})
    sim = pl.DataFrame({"joint_tour": [True, False]})
    res = compute_joint_tours_shares(survey, sim)
    assert res["distance"]["simulation"] is None
    assert res["tour_count"]["simulation"] == pytest.approx(0.5)


def test_plot_joint_share_comparison():
    fig = plot_joint_share_comparison(0.2, 0.3, "Share of tours")
    assert fig is not None


def test_restrict_to_home_in_area():
    import shapely

    from pymetropolis.metro_calibration.survey.joint_travel import restrict_to_home_in_area

    df = pl.DataFrame(
        {"tour_id": [1, 2, 3], "home_lng": [0.5, 5.0, None], "home_lat": [0.5, 0.5, None]}
    )
    area = shapely.box(0, 0, 1, 1)
    assert restrict_to_home_in_area(df, area)["tour_id"].to_list() == [1]
