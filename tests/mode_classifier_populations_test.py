"""The mode classifier is specific to each population: a single-mode population without classifier
gets its single mode for all tours, even when another population uses a classifier."""

from pathlib import Path

from pymetropolis.common import ThreadedStep
from pymetropolis.metro_calibration.survey.files import JointTourEstimatorFile, ModeEstimatorFile
from pymetropolis.metro_calibration.survey.joint_travel import ExternalJointToursClassifierStep
from pymetropolis.metro_calibration.survey.modes import (
    ClassifyToursModeStep,
    ExternalModeClassifierStep,
    SingleModeToursStep,
)
from pymetropolis.metro_demand.population.files import ToursFile, ToursModeFile
from pymetropolis.metro_pipeline import Config, PopulationStep, Step
from pymetropolis.metro_pipeline.graph import build_step_graph


class DummyToursStep(PopulationStep):
    output_files = {"tours": ToursFile}

    def run(self):
        pass


STEPS: list[type[Step]] = [
    DummyToursStep,
    ExternalModeClassifierStep,
    ClassifyToursModeStep,
    SingleModeToursStep,
]


def make_config(tmp: Path, main: dict, trucks: str) -> Config:
    model_path = tmp / "model.joblib"
    model_path.touch()
    (tmp / "trucks.toml").write_text(f'population_name = "trucks"\n{trucks}')
    return Config(
        {"main_directory": str(tmp / "out"), "extra_populations": [str(tmp / "trucks.toml")]} | main
    )


def tours_mode_producers(config: Config) -> dict[str, str]:
    """Returns, for each population, the name of the Step generating its `ToursModeFile`."""
    graph = build_step_graph(config, STEPS)
    producers = dict()
    for file, steps in graph.generated_files.items():
        if isinstance(file, ToursModeFile):
            assert len(steps) == 1
            step = next(iter(steps))
            assert isinstance(step, PopulationStep)
            producers[step.population_name] = str(step)
    return producers


def test_mixed_populations(tmp_path: Path):
    config = make_config(
        tmp_path,
        {
            "mode_choice": {"modes": ["car_driver", "walking"]},
            "mode_classifier": {"external_model": str(tmp_path / "model.joblib")},
        },
        '[mode_choice]\nmodes = ["truck"]\n',
    )
    producers = tours_mode_producers(config)
    assert producers == {
        "population": "ClassifyToursModeStep",
        "trucks": "trucks__SingleModeToursStep",
    }
    assert not ExternalModeClassifierStep.for_population(config, "trucks").is_defined()


def test_single_mode_with_classifier(tmp_path: Path):
    # A single-mode population with a classifier: modes are drawn from the classifier.
    config = make_config(
        tmp_path,
        {
            "mode_choice": {"modes": ["car_driver"]},
            "mode_classifier": {"external_model": str(tmp_path / "model.joblib")},
        },
        '[mode_choice]\nmodes = ["truck"]\n',
    )
    producers = tours_mode_producers(config)
    assert producers == {
        "population": "ClassifyToursModeStep",
        "trucks": "trucks__SingleModeToursStep",
    }


def test_single_mode_without_classifier(tmp_path: Path):
    config = make_config(
        tmp_path, {"mode_choice": {"modes": ["car_driver"]}}, '[mode_choice]\nmodes = ["truck"]\n'
    )
    producers = tours_mode_producers(config)
    assert producers == {
        "population": "SingleModeToursStep",
        "trucks": "trucks__SingleModeToursStep",
    }


def test_estimator_paths_differ_per_population(tmp_path: Path):
    config = make_config(tmp_path, {}, "")
    for step_class, file_class in (
        (ExternalModeClassifierStep, ModeEstimatorFile),
        (ExternalJointToursClassifierStep, JointTourEstimatorFile),
    ):
        main = step_class(config).output["estimator"]
        trucks = step_class.for_population(config, "trucks").output["estimator"]
        assert isinstance(main, file_class)
        assert main.complete_path != trucks.complete_path


class DummyThreadedPopulationStep(ThreadedStep, PopulationStep):
    def run(self):
        pass


def test_nb_threads_falls_back_to_main_config(tmp_path: Path):
    config = make_config(tmp_path, {"nb_threads": 3}, "")
    assert DummyThreadedPopulationStep.for_population(config, "trucks").nb_threads == 3
