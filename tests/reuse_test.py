"""Tests for the per-step reuse decision (`MetroPipeline.source_config`, computed by
`pymetropolis.metro_pipeline.reuse.compute_source_config`).

These are exercised through `MetroPipeline` itself (built, and sometimes actually run, against
real temp-dir configs) rather than by calling `compute_source_config` directly: building the
`steps`/`generated_files` graph by hand for every scenario would be far more tedious than letting
`MetroPipeline` do it, and this also covers the wiring in `MetroPipeline.__init__`.
"""

import tempfile
from pathlib import Path

from pymetropolis.metro_pipeline import Config, MetroPipeline, PopulationStep, Step
from pymetropolis.metro_pipeline.file import MetroTxtFile, PopulationFile
from pymetropolis.metro_pipeline.parameters import IntParameter, StringParameter


def _write_toml(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


class SrcFile(MetroTxtFile):
    path = "src.txt"


class DerivedFile(MetroTxtFile):
    path = "derived.txt"


class SrcStep(Step):
    output_files = {"out": SrcFile}
    value = StringParameter("dep.value", default="v")

    def run(self):
        self.output["out"].write(self.value)


class DerivedStep(Step):
    input_files = {"in": SrcFile}
    output_files = {"out": DerivedFile}
    factor = StringParameter("dep.factor", default="f")

    def run(self):
        self.output["out"].write(self.input["in"].read() + self.factor)


STEP_CLASSES: list[type[Step]] = [SrcStep, DerivedStep]


class PopOutFile(MetroTxtFile, PopulationFile):
    path = "{population}/out.txt"


class PopStep(PopulationStep):
    output_files = {"out": PopOutFile}
    value = IntParameter("dep.number", shared=True, default=1)

    def run(self):
        self.output["out"].write(str(self.value))


def _run(config: Config, step_classes: list[type[Step]] = STEP_CLASSES) -> MetroPipeline:
    pipeline = MetroPipeline(config, step_classes)
    pipeline.run()
    return pipeline


def _build(config: Config, step_classes: list[type[Step]] = STEP_CLASSES) -> MetroPipeline:
    return MetroPipeline(config, step_classes)


def _step(pipeline: MetroPipeline, cls: type[Step], population: str | None = None) -> Step:
    for step in pipeline.steps:
        if type(step) is cls and getattr(step, "population_name", None) in (population, None):
            return step
    raise AssertionError(f"Step {cls.__name__} not found in pipeline (population={population!r})")


def _source_dir(pipeline: MetroPipeline, cls: type[Step], population: str | None = None) -> Path:
    """The `main_directory` `pipeline` resolved as the source of `cls` (for `population`).

    Compared by `main_directory` rather than by `Config` identity: `Config.from_toml` builds a
    fresh `Config` object for the parent config every time it is called (here, once directly in
    the test, once again while loading the derived config), so the two never compare `is`-equal
    even when they represent the exact same config file / directory.
    """
    return pipeline.source_config[_step(pipeline, cls, population)].main_directory


def test_reused_when_identical_and_ancestor_fresh():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(
            tmp / "parent" / "config.toml",
            'main_directory = "out"\n[dep]\nvalue = "v"\nfactor = "f"\n',
        )
        parent = _run(Config.from_toml(tmp / "parent" / "config.toml"))

        _write_toml(
            tmp / "derived" / "config.toml",
            'main_directory = "out"\nparent_config = "../parent/config.toml"\n',
        )
        derived = _build(Config.from_toml(tmp / "derived" / "config.toml"))

        assert _source_dir(derived, SrcStep) == parent.config.main_directory
        assert _source_dir(derived, DerivedStep) == parent.config.main_directory


def test_not_reused_when_own_param_changed():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(
            tmp / "parent" / "config.toml",
            'main_directory = "out"\n[dep]\nvalue = "v"\nfactor = "f"\n',
        )
        parent = _run(Config.from_toml(tmp / "parent" / "config.toml"))

        _write_toml(
            tmp / "derived" / "config.toml",
            'main_directory = "out"\nparent_config = "../parent/config.toml"\n'
            '[dep]\nfactor = "other"\n',
        )
        derived = _build(Config.from_toml(tmp / "derived" / "config.toml"))

        # SrcStep does not read `dep.factor`, so it is unaffected and still reusable.
        assert _source_dir(derived, SrcStep) == parent.config.main_directory
        # DerivedStep reads `dep.factor`, which changed: it must be recomputed by `derived`.
        assert _source_dir(derived, DerivedStep) == derived.config.main_directory


def test_not_reused_when_ancestor_cache_stale():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(
            tmp / "parent" / "config.toml",
            'main_directory = "out"\n[dep]\nvalue = "v"\nfactor = "f"\n',
        )
        parent = _run(Config.from_toml(tmp / "parent" / "config.toml"))
        # Simulate a parent whose own cache no longer reflects its directory (e.g. its own
        # `main_directory` was tampered with, or it was never actually run to completion).
        (parent.config.main_directory / "update_files" / "SrcStep.json").unlink()

        _write_toml(
            tmp / "derived" / "config.toml",
            'main_directory = "out"\nparent_config = "../parent/config.toml"\n',
        )
        derived = _build(Config.from_toml(tmp / "derived" / "config.toml"))

        # SrcStep's own ancestor cache is stale: not reusable.
        assert _source_dir(derived, SrcStep) == derived.config.main_directory
        # DerivedStep's own params/cache are still fine, but its input (SrcFile) is no longer
        # trustworthy from the parent either, since SrcStep itself fell back to `derived`.
        assert _source_dir(derived, DerivedStep) == derived.config.main_directory


def test_not_reused_when_upstream_input_diverges_at_intermediate_config():
    """`b` changes `dep.value` (read by `SrcStep`, not by `DerivedStep`) relative to `a`. Even
    though `DerivedStep`'s own parameters are unaffected, its input file's content now differs
    from `a`'s, so `c` (identical to `b`) must not skip past `b` to reuse `DerivedStep` from `a`.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(
            tmp / "a" / "config.toml", 'main_directory = "out"\n[dep]\nvalue = "v"\nfactor = "f"\n'
        )
        _run(Config.from_toml(tmp / "a" / "config.toml"))

        _write_toml(
            tmp / "b" / "config.toml",
            'main_directory = "out"\nparent_config = "../a/config.toml"\n[dep]\nvalue = "other"\n',
        )
        b = _run(Config.from_toml(tmp / "b" / "config.toml"))
        # `DerivedStep`'s own config_hash matches `a`'s (its only param, `dep.factor`, is
        # unchanged), yet it must have actually run fresh in `b`'s own directory because its
        # input diverged.
        assert _source_dir(b, SrcStep) == b.config.main_directory
        assert _source_dir(b, DerivedStep) == b.config.main_directory

        _write_toml(
            tmp / "c" / "config.toml",
            'main_directory = "out"\nparent_config = "../b/config.toml"\n',
        )
        c = _build(Config.from_toml(tmp / "c" / "config.toml"))
        assert _source_dir(c, SrcStep) == b.config.main_directory
        assert _source_dir(c, DerivedStep) == b.config.main_directory


def test_reused_transitively_through_unrun_intermediate_config():
    """`b` inherits from `a` without overriding anything the Steps read, and is never itself run.
    `c` (identical to `b`) must still resolve all the way back to `a`, `b`'s own cache being
    irrelevant since `b`'s steps are themselves fully inherited from `a`.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(
            tmp / "a" / "config.toml", 'main_directory = "out"\n[dep]\nvalue = "v"\nfactor = "f"\n'
        )
        a = _run(Config.from_toml(tmp / "a" / "config.toml"))

        _write_toml(
            tmp / "b" / "config.toml",
            'main_directory = "out"\nparent_config = "../a/config.toml"\n[unrelated]\nkey = 1\n',
        )
        _write_toml(
            tmp / "c" / "config.toml",
            'main_directory = "out"\nparent_config = "../b/config.toml"\n',
        )
        c = _build(Config.from_toml(tmp / "c" / "config.toml"))

        assert _source_dir(c, SrcStep) == a.config.main_directory
        assert _source_dir(c, DerivedStep) == a.config.main_directory


def test_population_step_reused_only_when_population_matches_in_ancestor():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write_toml(tmp / "parent" / "config.toml", 'main_directory = "out"\n')
        parent = _run(Config.from_toml(tmp / "parent" / "config.toml"), [PopStep])

        _write_toml(tmp / "derived" / "trucks.toml", 'population_name = "trucks"\n')
        _write_toml(
            tmp / "derived" / "config.toml",
            'main_directory = "out"\nparent_config = "../parent/config.toml"\n'
            'extra_populations = ["trucks.toml"]\n',
        )
        derived = _build(Config.from_toml(tmp / "derived" / "config.toml"), [PopStep])

        # The main population is unchanged and reusable from the parent.
        assert _source_dir(derived, PopStep, "population") == parent.config.main_directory
        # The "trucks" population only exists in `derived`'s own chain segment: no equivalent step
        # in the parent, so nothing to reuse.
        assert _source_dir(derived, PopStep, "trucks") == derived.config.main_directory
