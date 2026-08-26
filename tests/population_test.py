import tempfile
from pathlib import Path

import pytest

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_pipeline import Config, MetroPipeline, PopulationStep, Step
from pymetropolis.metro_pipeline.file import MetroDataFrameFile, MetroTxtFile, PopulationFile
from pymetropolis.metro_pipeline.parameters import StringParameter
from pymetropolis.metro_pipeline.steps import MAIN_POPULATION_NAME, InputFile
from pymetropolis.metro_simulation.common import merge_populations
from pymetropolis.random import RandomStep


def _write_toml(path: Path, content: str):
    path.write_text(content)


class PopFile1(MetroTxtFile, PopulationFile):
    path = "pop1/{population}/file1.txt"


class PopFile2(MetroTxtFile, PopulationFile):
    path = "pop2/{population}/file2.txt"


class GlobalFile(MetroTxtFile):
    path = "global.txt"


# ---------------------------------------------------------------------------
# PopulationFile
# ---------------------------------------------------------------------------


def test_population_file_resolves_path():
    cls = PopFile1.for_population("trucks")
    assert cls.path == "pop1/trucks/file1.txt"


def test_population_file_is_memoized():
    a = PopFile1.for_population("trucks")
    b = PopFile1.for_population("trucks")
    assert a is b


def test_population_file_no_cross_class_collision():
    """Two different PopulationFile classes resolving the same population name must not collide
    (regression test: `_population_variants` used to be a single dict shared by every subclass).
    """
    a = PopFile1.for_population("trucks")
    b = PopFile2.for_population("trucks")
    assert a is not b
    assert a.path != b.path


def test_population_file_without_placeholder_raises():
    class BadFile(MetroTxtFile, PopulationFile):
        path = "no_placeholder.txt"

    with pytest.raises(MetropyError):
        BadFile.for_population("trucks")


# ---------------------------------------------------------------------------
# Config: extra populations / main_population
# ---------------------------------------------------------------------------


def test_extra_population_duplicate_name_rejected():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "a.toml", 'population_name = "trucks"\n')
        _write_toml(tmp / "b.toml", 'population_name = "trucks"\n')
        main_dict = {
            "main_directory": str(tmp / "out"),
            "extra_populations": [str(tmp / "a.toml"), str(tmp / "b.toml")],
        }
        with pytest.raises(MetropyError):
            Config(main_dict)


def test_extra_population_dash_in_name_rejected():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "a.toml", 'population_name = "east-side"\n')
        main_dict = {"main_directory": str(tmp / "out"), "extra_populations": [str(tmp / "a.toml")]}
        with pytest.raises(MetropyError):
            Config(main_dict)


def test_extra_population_reserved_main_name_rejected():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "a.toml", f'population_name = "{MAIN_POPULATION_NAME}"\n')
        main_dict = {"main_directory": str(tmp / "out"), "extra_populations": [str(tmp / "a.toml")]}
        with pytest.raises(MetropyError):
            Config(main_dict)


def test_main_population_defaults_to_true():
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config({"main_directory": tmp_dir})
        assert config.main_population is True


def test_main_population_can_be_disabled():
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config({"main_directory": tmp_dir, "main_population": False})
        assert config.main_population is False


def test_main_population_invalid_type_rejected():
    with tempfile.TemporaryDirectory() as tmp_dir:
        with pytest.raises(MetropyError):
            Config({"main_directory": tmp_dir, "main_population": "yes"})


# ---------------------------------------------------------------------------
# shared vs non-shared parameter resolution
# ---------------------------------------------------------------------------


class DummyRandomPopulationStep(RandomStep, PopulationStep):
    def run(self):
        pass


class DummyNonSharedParamStep(PopulationStep):
    value = StringParameter("some_section.value")

    def run(self):
        pass


def test_shared_parameter_falls_back_to_main_config():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "trucks.toml", 'population_name = "trucks"\n')
        config = Config(
            {
                "main_directory": str(tmp / "out"),
                "random_seed": 42,
                "extra_populations": [str(tmp / "trucks.toml")],
            }
        )
        step = DummyRandomPopulationStep.for_population(config, "trucks")
        assert step.random_seed == 42


def test_non_shared_parameter_does_not_fall_back():
    """A non-`shared` parameter must resolve to None for an extra population that doesn't define
    it itself, even though the main config happens to define the same key.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "trucks.toml", 'population_name = "trucks"\n')
        config = Config(
            {
                "main_directory": str(tmp / "out"),
                "some_section": {"value": "from-main"},
                "extra_populations": [str(tmp / "trucks.toml")],
            }
        )
        step = DummyNonSharedParamStep.for_population(config, "trucks")
        assert step.value is None


# ---------------------------------------------------------------------------
# PopulationStep naming / cache path
# ---------------------------------------------------------------------------


class DummyProducerStep(PopulationStep):
    output_files = {"f": PopFile1}

    def run(self):
        pass


def test_population_step_str_and_cache_path_differ_per_population():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "trucks.toml", 'population_name = "trucks"\n')
        config = Config(
            {"main_directory": str(tmp / "out"), "extra_populations": [str(tmp / "trucks.toml")]}
        )
        main_instance = DummyProducerStep(config)
        trucks_instance = DummyProducerStep.for_population(config, "trucks")

        assert str(main_instance) == "DummyProducerStep"
        assert str(trucks_instance) == "trucks__DummyProducerStep"
        assert main_instance._update_file_path != trucks_instance._update_file_path
        assert main_instance.output["f"].complete_path != trucks_instance.output["f"].complete_path


# ---------------------------------------------------------------------------
# __init_subclass__ guards
# ---------------------------------------------------------------------------


def test_non_population_step_rejects_bare_population_file_input():
    with pytest.raises(MetropyError):

        class Bad(Step):
            input_files = {"f": PopFile1}


def test_non_population_step_rejects_population_file_output():
    with pytest.raises(MetropyError):

        class Bad(Step):
            output_files = {"f": PopFile1}


def test_non_population_step_allows_all_populations_input():
    class Ok(Step):
        input_files = {"f": InputFile(PopFile1, all_populations=True)}

    assert Ok.input_files["f"].all_populations is True


def test_population_step_rejects_non_population_file_output():
    with pytest.raises(MetropyError):

        class Bad(PopulationStep):
            output_files = {"f": GlobalFile}

            def run(self):
                pass


def test_population_step_allows_non_population_file_input():
    class Ok(PopulationStep):
        input_files = {"f": GlobalFile}
        output_files = {"g": PopFile1}

        def run(self):
            pass

    assert Ok.input_files["f"] is GlobalFile


def test_population_step_rejects_init_override():
    with pytest.raises(MetropyError):

        class Bad(PopulationStep):
            def __init__(self, config):
                super().__init__(config)


# ---------------------------------------------------------------------------
# MetroPipeline integration
# ---------------------------------------------------------------------------


class ProducerFile(MetroTxtFile, PopulationFile):
    path = "demand/{population}/produced.txt"


class MergedFile(MetroTxtFile):
    path = "merged.txt"


class ProducerStep(PopulationStep):
    output_files = {"out": ProducerFile}

    def run(self):
        self.output["out"].write(str(self.population_name))


class MergerStep(Step):
    input_files = {"pop": InputFile(ProducerFile, all_populations=True)}
    output_files = {"merged": MergedFile}

    def run(self):
        values = sorted(f.read() for f in self.input_populations["pop"].values())
        self.output["merged"].write(",".join(values))


def test_population_step_runs_once_per_population_and_merge_step_combines_them():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "trucks.toml", 'population_name = "trucks"\n')
        config = Config(
            {"main_directory": str(tmp / "out"), "extra_populations": [str(tmp / "trucks.toml")]}
        )
        pipeline = MetroPipeline(config, [ProducerStep, MergerStep])
        pipeline.run()

        main_file = ProducerFile.for_population(MAIN_POPULATION_NAME).from_dir(
            config.main_directory
        )
        trucks_file = ProducerFile.for_population("trucks").from_dir(config.main_directory)
        assert main_file.read() == MAIN_POPULATION_NAME
        assert trucks_file.read() == "trucks"

        merged_content = MergedFile.from_dir(config.main_directory).read()
        assert merged_content == f"{MAIN_POPULATION_NAME},trucks"


def test_main_population_false_excludes_main_population_from_pipeline():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "trucks.toml", 'population_name = "trucks"\n')
        config = Config(
            {
                "main_directory": str(tmp / "out"),
                "main_population": False,
                "extra_populations": [str(tmp / "trucks.toml")],
            }
        )
        pipeline = MetroPipeline(config, [ProducerStep, MergerStep])
        step_names = sorted(str(s) for s in pipeline.steps.keys())
        assert step_names == ["MergerStep", "trucks__ProducerStep"]

        pipeline.run()
        merged_content = MergedFile.from_dir(config.main_directory).read()
        assert merged_content == "trucks"


# ---------------------------------------------------------------------------
# RandomStep.get_rng
# ---------------------------------------------------------------------------


class DummyRandomStepA(RandomStep):
    def run(self):
        pass


class DummyRandomStepB(RandomStep):
    def run(self):
        pass


def test_get_rng_decorrelates_across_steps_and_is_reproducible():
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config({"main_directory": tmp_dir, "random_seed": 123})

        a = DummyRandomStepA(config)
        b = DummyRandomStepB(config)
        draws_a = a.get_rng(str(a)).random(5)
        draws_b = b.get_rng(str(b)).random(5)
        assert (draws_a != draws_b).any()

        a2 = DummyRandomStepA(config)
        draws_a2 = a2.get_rng(str(a2)).random(5)
        assert (draws_a == draws_a2).all()


def test_get_rng_without_seed_is_not_reproducible():
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config({"main_directory": tmp_dir})
        a = DummyRandomStepA(config)
        draws_1 = a.get_rng(str(a)).random(5)
        draws_2 = a.get_rng(str(a)).random(5)
        assert (draws_1 != draws_2).any()


# ---------------------------------------------------------------------------
# merge_populations
# ---------------------------------------------------------------------------


class MergeTestFile(MetroDataFrameFile):
    path = "merge_test_dummy.parquet"


def _write_df(tmp_dir: str, df) -> MergeTestFile:
    f = MergeTestFile.from_dir(Path(tmp_dir))
    f.write(df)
    return f


def test_merge_populations_prefixes_ids_and_concatenates():
    import polars as pl

    with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
        files = {
            "population": _write_df(tmp_a, pl.DataFrame({"agent_id": [1, 2]})),
            "trucks": _write_df(tmp_b, pl.DataFrame({"agent_id": [1, 3]})),
        }
        merged = merge_populations(files)
        assert merged["agent_id"].to_list() == [
            "population-1",
            "population-2",
            "trucks-1",
            "trucks-3",
        ]


def test_merge_populations_with_multiple_id_columns():
    import polars as pl

    with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
        files = {
            "population": _write_df(tmp_a, pl.DataFrame({"agent_id": [1], "trip_id": [10]})),
            "trucks": _write_df(tmp_b, pl.DataFrame({"agent_id": [1], "trip_id": [10]})),
        }
        merged = merge_populations(files, id_columns=("agent_id", "trip_id"))
        assert merged["agent_id"].to_list() == ["population-1", "trucks-1"]
        assert merged["trip_id"].to_list() == ["population-10", "trucks-10"]
