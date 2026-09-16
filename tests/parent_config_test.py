import tempfile
from pathlib import Path

import pytest

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_pipeline import Config, MetroPipeline, PopulationStep, Step
from pymetropolis.metro_pipeline.file import MetroTxtFile, PopulationFile
from pymetropolis.metro_pipeline.parameters import (
    IntParameter,
    ListParameter,
    PathParameter,
    StringParameter,
)
from pymetropolis.metro_pipeline.types import PathType


def _write_toml(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _parent_and_derived(tmp: Path, parent_content: str, derived_content: str) -> Config:
    """Writes a parent config in `tmp/parent/` and a derived one in `tmp/derived/`, and reads the
    latter.

    The two configs live in different directories so that relative paths written in one of them
    cannot accidentally resolve against the other's directory.
    """
    _write_toml(tmp / "parent" / "config.toml", f'main_directory = "out"\n{parent_content}')
    _write_toml(
        tmp / "derived" / "config.toml",
        f'main_directory = "out"\nparent_config = "../parent/config.toml"\n{derived_content}',
    )
    return Config.from_toml(tmp / "derived" / "config.toml")


class ValueStep(Step):
    value = StringParameter("dep.value")
    other = StringParameter("dep.other")


class DataFileStep(Step):
    data_file = PathParameter("dep.data_file", check_file_exists=True)


class DataFilesStep(Step):
    data_files = ListParameter("dep.data_files", inner=PathType(check_file_exists=True))


class SharedValueStep(PopulationStep):
    value = StringParameter("dep.value", shared=True)

    def run(self):
        pass


class NonSharedValueStep(PopulationStep):
    value = StringParameter("dep.value")

    def run(self):
        pass


class PopOutFile(MetroTxtFile, PopulationFile):
    path = "{population}/out.txt"


class PopStep(PopulationStep):
    output_files = {"out": PopOutFile}
    value = IntParameter("dep.number", shared=True)

    def run(self):
        pass


# ---------------------------------------------------------------------------
# Value inheritance
# ---------------------------------------------------------------------------


def test_value_is_inherited_from_parent_config():
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = _parent_and_derived(Path(tmp_dir), '[dep]\nvalue = "from-parent"\n', "")
        assert ValueStep(config).value == "from-parent"


def test_value_is_overridden_by_derived_config():
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = _parent_and_derived(
            Path(tmp_dir), '[dep]\nvalue = "from-parent"\n', '[dep]\nvalue = "from-derived"\n'
        )
        assert ValueStep(config).value == "from-derived"


def test_tables_are_merged_key_by_key():
    """Restating one key of a table in the derived config must not hide the table's other keys."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = _parent_and_derived(
            Path(tmp_dir),
            '[dep]\nvalue = "from-parent"\nother = "other-from-parent"\n',
            '[dep]\nvalue = "from-derived"\n',
        )
        step = ValueStep(config)
        assert step.value == "from-derived"
        assert step.other == "other-from-parent"


# ---------------------------------------------------------------------------
# Path resolution (the reason the parent config is a Config and not a merged dict)
# ---------------------------------------------------------------------------


def test_relative_path_in_parent_config_is_resolved_against_parent_config_directory():
    """A relative path written in the parent config must resolve against the *parent* config's
    directory, not against the derived config's one.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        (tmp / "parent").mkdir()
        (tmp / "parent" / "data.csv").write_text("a,b\n")
        config = _parent_and_derived(tmp, '[dep]\ndata_file = "data.csv"\n', "")
        assert DataFileStep(config).data_file == tmp / "parent" / "data.csv"


def test_relative_path_in_derived_config_is_resolved_against_derived_config_directory():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        (tmp / "parent").mkdir()
        (tmp / "parent" / "data.csv").write_text("a,b\n")
        (tmp / "derived").mkdir()
        (tmp / "derived" / "data.csv").write_text("c,d\n")
        config = _parent_and_derived(
            tmp, '[dep]\ndata_file = "data.csv"\n', '[dep]\ndata_file = "data.csv"\n'
        )
        assert DataFileStep(config).data_file == tmp / "derived" / "data.csv"


def test_relative_paths_of_a_list_parameter_are_resolved_against_parent_config_directory():
    """Same as above for a `ListParameter(inner=PathType(...))`, i.e. the `gtfs.files` shape."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        (tmp / "parent").mkdir()
        (tmp / "parent" / "a.csv").write_text("a\n")
        (tmp / "parent" / "b.csv").write_text("b\n")
        config = _parent_and_derived(tmp, '[dep]\ndata_files = ["a.csv", "b.csv"]\n', "")
        assert DataFilesStep(config).data_files == [
            tmp / "parent" / "a.csv",
            tmp / "parent" / "b.csv",
        ]


def test_config_hash_is_identical_between_parent_and_derived_run():
    """The whole point of the feature: a step whose parameters all come from the parent config must
    hash identically in the parent run and in the derived run, so that the derived run can reuse the
    parent run's cached results.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        (tmp / "parent").mkdir()
        (tmp / "parent" / "data.csv").write_text("a,b\n")
        derived = _parent_and_derived(
            tmp, '[dep]\ndata_file = "data.csv"\n', '[unrelated]\nkey = "changed"\n'
        )
        parent = Config.from_toml(tmp / "parent" / "config.toml")
        assert DataFileStep(derived).config_hash() == DataFileStep(parent).config_hash()


# ---------------------------------------------------------------------------
# Secrets
# ---------------------------------------------------------------------------


def test_secret_in_parent_config_is_resolved_against_parent_secrets():
    """A `"secret:"` value written in the parent config is resolved against the secrets file of the
    parent config, not against the derived config's one.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "parent" / "secrets.toml", 'mysecret = "from-parent-secrets"\n')
        _write_toml(tmp / "derived" / "secrets.toml", 'mysecret = "from-derived-secrets"\n')
        config = _parent_and_derived(tmp, '[dep]\nvalue = "secret:mysecret"\n', "")
        assert ValueStep(config).value == "from-parent-secrets"


def test_secret_in_derived_config_is_resolved_against_derived_secrets():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "parent" / "secrets.toml", 'mysecret = "from-parent-secrets"\n')
        _write_toml(tmp / "derived" / "secrets.toml", 'mysecret = "from-derived-secrets"\n')
        config = _parent_and_derived(
            tmp, '[dep]\nvalue = "secret:mysecret"\n', '[dep]\nvalue = "secret:mysecret"\n'
        )
        assert ValueStep(config).value == "from-derived-secrets"


# ---------------------------------------------------------------------------
# Chain integrity
# ---------------------------------------------------------------------------


def test_self_referencing_parent_config_rejected():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "a.toml", 'main_directory = "out"\nparent_config = "a.toml"\n')
        with pytest.raises(MetropyError):
            Config.from_toml(tmp / "a.toml")


def test_cyclic_parent_config_rejected():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "a.toml", 'main_directory = "out-a"\nparent_config = "b.toml"\n')
        _write_toml(tmp / "b.toml", 'main_directory = "out-b"\nparent_config = "a.toml"\n')
        with pytest.raises(MetropyError):
            Config.from_toml(tmp / "a.toml")


def test_same_main_directory_as_parent_config_rejected():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "parent" / "config.toml", 'main_directory = "out"\n')
        _write_toml(
            tmp / "derived" / "config.toml",
            'main_directory = "../parent/out"\nparent_config = "../parent/config.toml"\n',
        )
        with pytest.raises(MetropyError):
            Config.from_toml(tmp / "derived" / "config.toml")


def test_missing_main_directory_in_derived_config_rejected():
    """`main_directory` is the one value which is never inherited."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "parent" / "config.toml", 'main_directory = "out"\n')
        _write_toml(tmp / "derived" / "config.toml", 'parent_config = "../parent/config.toml"\n')
        with pytest.raises(MetropyError):
            Config.from_toml(tmp / "derived" / "config.toml")


def test_missing_parent_config_file_rejected():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "a.toml", 'main_directory = "out"\nparent_config = "nope.toml"\n')
        with pytest.raises(MetropyError):
            Config.from_toml(tmp / "a.toml")


def test_parent_config_not_a_string_rejected():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "a.toml", 'main_directory = "out"\nparent_config = 42\n')
        with pytest.raises(MetropyError):
            Config.from_toml(tmp / "a.toml")


def test_chained_parent_configs():
    """`c.toml` inherits from `b.toml`, which inherits from `a.toml`: a value defined only in
    `a.toml` reaches `c.toml`, and the nearest config defining a value wins.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        (tmp / "a").mkdir()
        (tmp / "a" / "data.csv").write_text("a\n")
        _write_toml(
            tmp / "a" / "config.toml",
            'main_directory = "out"\n[dep]\nvalue = "from-a"\nother = "other-from-a"\n'
            'data_file = "data.csv"\n',
        )
        _write_toml(
            tmp / "b" / "config.toml",
            'main_directory = "out"\nparent_config = "../a/config.toml"\n[dep]\nvalue = "from-b"\n',
        )
        _write_toml(
            tmp / "c" / "config.toml",
            'main_directory = "out"\nparent_config = "../b/config.toml"\n',
        )
        config = Config.from_toml(tmp / "c" / "config.toml")
        step = ValueStep(config)
        assert step.value == "from-b"
        assert step.other == "other-from-a"
        # A relative path defined in the furthest config resolves against *its* directory.
        assert DataFileStep(config).data_file == tmp / "a" / "data.csv"


def test_parent_directories_are_listed_nearest_first():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "a" / "config.toml", 'main_directory = "out"\n')
        _write_toml(
            tmp / "b" / "config.toml",
            'main_directory = "out"\nparent_config = "../a/config.toml"\n',
        )
        _write_toml(
            tmp / "c" / "config.toml",
            'main_directory = "out"\nparent_config = "../b/config.toml"\n',
        )
        config = Config.from_toml(tmp / "c" / "config.toml")
        assert config.parent_directories == [tmp / "b" / "out", tmp / "a" / "out"]
        assert Config.from_toml(tmp / "a" / "config.toml").parent_directories == []


# ---------------------------------------------------------------------------
# Populations
# ---------------------------------------------------------------------------


def test_extra_populations_are_inherited():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "parent" / "trucks.toml", 'population_name = "trucks"\n')
        config = _parent_and_derived(
            tmp, 'extra_populations = ["trucks.toml"]\n[dep]\nnumber = 1\n', ""
        )
        assert config.population_names == ["trucks"]
        pipeline = MetroPipeline(config, [PopStep])
        assert sorted(map(str, pipeline.steps)) == ["PopStep", "trucks__PopStep"]


def test_extra_population_is_merged_by_name():
    """A population re-declared by the derived config overlays the parent config's declaration,
    key by key, instead of replacing it.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(
            tmp / "parent" / "trucks.toml",
            'population_name = "trucks"\n[dep]\nvalue = "from-parent"\n'
            'other = "other-from-parent"\n',
        )
        _write_toml(
            tmp / "derived" / "trucks.toml",
            'population_name = "trucks"\n[dep]\nvalue = "from-derived"\n',
        )
        config = _parent_and_derived(
            tmp, 'extra_populations = ["trucks.toml"]\n', 'extra_populations = ["trucks.toml"]\n'
        )
        assert config.population_names == ["trucks"]
        step = NonSharedValueStep.for_population(config, "trucks")
        assert step.value == "from-derived"
        assert config.resolve_parameter(["dep", "other"], "trucks") == "other-from-parent"


def test_population_names_list_parent_populations_first():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "parent" / "trucks.toml", 'population_name = "trucks"\n')
        _write_toml(tmp / "derived" / "bikes.toml", 'population_name = "bikes"\n')
        config = _parent_and_derived(
            tmp, 'extra_populations = ["trucks.toml"]\n', 'extra_populations = ["bikes.toml"]\n'
        )
        assert config.population_names == ["trucks", "bikes"]


def test_duplicate_population_name_within_one_config_still_rejected():
    """Re-declaring a population is only an override *across* configs, never within one."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "derived" / "a.toml", 'population_name = "trucks"\n')
        _write_toml(tmp / "derived" / "b.toml", 'population_name = "trucks"\n')
        with pytest.raises(MetropyError):
            _parent_and_derived(tmp, "", 'extra_populations = ["a.toml", "b.toml"]\n')


def test_population_config_of_parent_beats_main_config_of_derived():
    """A population-specific value always wins over a main-config value, whichever config of the
    chain each of them comes from.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(
            tmp / "parent" / "trucks.toml",
            'population_name = "trucks"\n[dep]\nvalue = "trucks-from-parent"\n',
        )
        config = _parent_and_derived(
            tmp, 'extra_populations = ["trucks.toml"]\n', '[dep]\nvalue = "from-derived-main"\n'
        )
        step = SharedValueStep.for_population(config, "trucks")
        assert step.value == "trucks-from-parent"
        # The main population still reads the derived config's value.
        assert SharedValueStep(config).value == "from-derived-main"


def test_shared_parameter_falls_back_to_parent_main_config():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "parent" / "trucks.toml", 'population_name = "trucks"\n')
        config = _parent_and_derived(
            tmp, 'extra_populations = ["trucks.toml"]\n[dep]\nvalue = "from-parent-main"\n', ""
        )
        assert SharedValueStep.for_population(config, "trucks").value == "from-parent-main"


def test_non_shared_parameter_falls_back_to_parent_population_config():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(
            tmp / "parent" / "trucks.toml",
            'population_name = "trucks"\n[dep]\nvalue = "trucks-from-parent"\n',
        )
        config = _parent_and_derived(tmp, 'extra_populations = ["trucks.toml"]\n', "")
        assert NonSharedValueStep.for_population(config, "trucks").value == "trucks-from-parent"


def test_non_shared_parameter_does_not_fall_back_to_any_main_config():
    """The existing rule still holds once a parent config is involved: a non-shared parameter is
    never read from a main config for an extra population.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "parent" / "trucks.toml", 'population_name = "trucks"\n')
        config = _parent_and_derived(
            tmp,
            'extra_populations = ["trucks.toml"]\n[dep]\nvalue = "from-parent-main"\n',
            '[dep]\nvalue = "from-derived-main"\n',
        )
        assert NonSharedValueStep.for_population(config, "trucks").value is None


def test_unknown_population_name_rejected():
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = _parent_and_derived(Path(tmp_dir), "", "")
        with pytest.raises(MetropyError):
            config.resolve_parameter(["dep", "value"], "nope")


def test_main_population_is_inherited():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "parent" / "trucks.toml", 'population_name = "trucks"\n')
        config = _parent_and_derived(
            tmp, 'main_population = false\nextra_populations = ["trucks.toml"]\n', ""
        )
        assert config.main_population is False


def test_derived_config_can_re_enable_main_population():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "parent" / "trucks.toml", 'population_name = "trucks"\n')
        config = _parent_and_derived(
            tmp,
            'main_population = false\nextra_populations = ["trucks.toml"]\n',
            "main_population = true\n",
        )
        assert config.main_population is True


# ---------------------------------------------------------------------------
# Custom steps / unused keys
# ---------------------------------------------------------------------------


_CUSTOM_STEP_parent = """
from pymetropolis.metro_pipeline import Step


class InheritedStep(Step):
    pass


class OverriddenStep(Step):
    origin = "parent"
"""

_CUSTOM_STEP_DERIVED = """
from pymetropolis.metro_pipeline import Step


class OverriddenStep(Step):
    origin = "derived"
"""


def test_custom_steps_are_concatenated_parent_first():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        (tmp / "parent" / "steps.py").parent.mkdir(parents=True, exist_ok=True)
        (tmp / "parent" / "steps.py").write_text(_CUSTOM_STEP_parent)
        (tmp / "derived" / "steps.py").parent.mkdir(parents=True, exist_ok=True)
        (tmp / "derived" / "steps.py").write_text(_CUSTOM_STEP_DERIVED)
        config = _parent_and_derived(
            tmp, 'custom_steps = ["steps.py"]\n', 'custom_steps = ["steps.py"]\n'
        )
        assert config.custom_step_paths == [
            tmp / "parent" / "steps.py",
            tmp / "derived" / "steps.py",
        ]
        pipeline = MetroPipeline(config, [])
        by_name = {cls.__name__: cls for cls in pipeline.load_custom_steps([])}
        assert "InheritedStep" in by_name
        assert getattr(by_name["OverriddenStep"], "origin") == "derived"


def test_custom_step_file_declared_twice_is_loaded_once():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        (tmp / "parent").mkdir(parents=True, exist_ok=True)
        (tmp / "parent" / "steps.py").write_text(_CUSTOM_STEP_parent)
        config = _parent_and_derived(
            tmp, 'custom_steps = ["steps.py"]\n', 'custom_steps = ["../parent/steps.py"]\n'
        )
        assert len(config.custom_step_paths) == 1


def test_parent_config_key_is_not_reported_as_unused():
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = _parent_and_derived(Path(tmp_dir), "", "")
        assert config.get_unused_keys(set()) == set()
