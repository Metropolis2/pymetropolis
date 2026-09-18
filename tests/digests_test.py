import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path

from pymetropolis.metro_pipeline import Config, MetroPipeline, Step
from pymetropolis.metro_pipeline.digests import MISSING_DIGEST, DigestCache
from pymetropolis.metro_pipeline.file import MetroTxtFile
from pymetropolis.metro_pipeline.parameters import (
    ExecPathParameter,
    ListParameter,
    PathParameter,
    StringParameter,
)
from pymetropolis.metro_pipeline.pipeline import StepStatus
from pymetropolis.metro_pipeline.types import PathType


def _cache(tmp: Path) -> DigestCache:
    return DigestCache(tmp / "digest_cache.json")


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


# ---------------------------------------------------------------------------
# DigestCache
# ---------------------------------------------------------------------------


def test_same_content_at_different_paths_has_the_same_digest():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        a = _write(tmp / "a" / "data.csv", "x,y\n")
        b = _write(tmp / "b" / "other-name.csv", "x,y\n")
        cache = _cache(tmp)
        assert cache.digest_path(a) == cache.digest_path(b)


def test_digest_changes_with_content():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        f = _write(tmp / "data.csv", "x,y\n")
        cache = _cache(tmp)
        before = cache.digest_path(f)
        _write(f, "x,y,z\n")
        assert _cache(tmp).digest_path(f) != before


def test_missing_path_yields_the_missing_sentinel():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        assert _cache(tmp).digest_path(tmp / "nope.csv") == MISSING_DIGEST


def test_directory_digest_changes_when_a_nested_file_changes():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        d = tmp / "dir"
        _write(d / "sub" / "a.csv", "a\n")
        _write(d / "b.csv", "b\n")
        before = _cache(tmp).digest_path(d)
        _write(d / "sub" / "a.csv", "changed\n")
        assert _cache(tmp).digest_path(d) != before


def test_directory_digest_changes_when_a_file_is_added_or_renamed():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        d = tmp / "dir"
        _write(d / "a.csv", "a\n")
        before = _cache(tmp).digest_path(d)
        _write(d / "b.csv", "b\n")
        with_extra = _cache(tmp).digest_path(d)
        assert with_extra != before
        (d / "b.csv").rename(d / "c.csv")
        assert _cache(tmp).digest_path(d) != with_extra


def test_directory_digest_is_stable_when_the_directory_is_moved():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write(tmp / "dir" / "sub" / "a.csv", "a\n")
        before = _cache(tmp).digest_path(tmp / "dir")
        shutil.move(tmp / "dir", tmp / "moved")
        assert _cache(tmp).digest_path(tmp / "moved") == before


def test_unchanged_file_is_not_read_twice(monkeypatch):
    """The whole design rests on this: a run which changes nothing must only `stat` the files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        f = _write(tmp / "data.csv", "x,y\n")
        cache = _cache(tmp)
        cache.digest_path(f)

        calls = 0
        real = hashlib.file_digest

        def counting(*args, **kwargs):
            nonlocal calls
            calls += 1
            return real(*args, **kwargs)

        monkeypatch.setattr(hashlib, "file_digest", counting)
        cache.digest_path(f)
        assert calls == 0
        # ... but a modified file is re-read.
        _write(f, "x,y,z\n")
        cache.digest_path(f)
        assert calls == 1


def test_rewriting_identical_content_yields_the_same_digest():
    """A file whose mtime changed but whose content did not must not invalidate anything.

    This is a deliberate improvement over the modification-time check it replaces.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        f = _write(tmp / "data.csv", "x,y\n")
        cache = _cache(tmp)
        before = cache.digest_path(f)
        f.touch()
        assert cache.digest_path(f) == before


def test_corrupted_cache_file_is_discarded():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        f = _write(tmp / "data.csv", "x,y\n")
        (tmp / "digest_cache.json").write_text("{not valid json")
        cache = _cache(tmp)
        assert cache.digest_path(f) != MISSING_DIGEST


def test_cache_round_trips_through_the_cache_file():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        f = _write(tmp / "data.csv", "x,y\n")
        cache = _cache(tmp)
        digest = cache.digest_path(f)
        cache.save()
        entries = json.loads((tmp / "digest_cache.json").read_text())
        assert entries[str(f)]["digest"] == digest
        assert _cache(tmp).digest_path(f) == digest


def test_empty_directory_and_empty_file_have_different_digests():
    """Without a tag seeding the directory hasher, both would be the digest of an empty input."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        empty_dir = tmp / "dir"
        empty_dir.mkdir()
        empty_file = _write(tmp / "empty.csv", "")
        cache = _cache(tmp)
        assert cache.digest_path(empty_dir) != cache.digest_path(empty_file)


def test_directory_digest_ignores_the_directory_own_mtime():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        d = tmp / "dir"
        _write(d / "a.csv", "a\n")
        before = _cache(tmp).digest_path(d)
        os.utime(d, (0, 0))
        assert _cache(tmp).digest_path(d) == before


def test_saved_cache_only_keeps_the_paths_consulted():
    """The cache must not grow forever as the content of a digested directory changes."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        stale = _write(tmp / "stale.csv", "stale\n")
        cache = _cache(tmp)
        cache.digest_path(stale)
        cache.save()
        assert str(stale) in json.loads((tmp / "digest_cache.json").read_text())

        fresh = _write(tmp / "fresh.csv", "fresh\n")
        reloaded = _cache(tmp)
        reloaded.digest_path(fresh)
        reloaded.save()
        entries = json.loads((tmp / "digest_cache.json").read_text())
        assert str(fresh) in entries
        assert str(stale) not in entries


def test_digest_value_only_touches_paths():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        f = _write(tmp / "data.csv", "x,y\n")
        cache = _cache(tmp)
        assert cache.digest_value("a string") == "a string"
        assert cache.digest_value(42) == 42
        assert cache.digest_value(None) is None
        assert cache.digest_value([1, 2]) == [1, 2]
        assert cache.digest_value(f) == cache.digest_path(f)
        assert cache.digest_value([f, f]) == [cache.digest_path(f)] * 2


# ---------------------------------------------------------------------------
# config_hash
# ---------------------------------------------------------------------------


class DataFileStep(Step):
    data_file = PathParameter("dep.data_file")


class DataFilesStep(Step):
    """The `gtfs.files` shape: a list of paths, which the previous mtime check did not cover."""

    data_files = ListParameter("dep.data_files", inner=PathType())


class DataDirStep(Step):
    data_dir = PathParameter("dep.data_dir", check_dir_exists=True)


class ExecStep(Step):
    exec_path = ExecPathParameter("dep.exec_path")


def _config(tmp: Path, **dep) -> Config:
    return Config({"main_directory": str(tmp / "out"), "dep": dep})


def test_config_hash_is_identical_on_two_machines():
    """The same project synchronized to a different absolute location must keep its cache.

    This is what the path-based hash could not do, and the reason for this whole change.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        _write(tmp / "machine_a" / "data.csv", "x,y\n")
        shutil.copytree(tmp / "machine_a", tmp / "machine_b")
        a = _config(tmp / "machine_a", data_file=str(tmp / "machine_a" / "data.csv"))
        b = _config(tmp / "machine_b", data_file=str(tmp / "machine_b" / "data.csv"))
        assert DataFileStep(a).config_hash() == DataFileStep(b).config_hash()


def test_config_hash_changes_when_a_data_file_is_edited_in_place():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        f = _write(tmp / "data.csv", "x,y\n")
        before = DataFileStep(_config(tmp, data_file=str(f))).config_hash()
        _write(f, "x,y,z\n")
        assert DataFileStep(_config(tmp, data_file=str(f))).config_hash() != before


def test_config_hash_is_unchanged_when_a_data_file_is_moved():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        f = _write(tmp / "data.csv", "x,y\n")
        before = DataFileStep(_config(tmp, data_file=str(f))).config_hash()
        moved = tmp / "elsewhere" / "renamed.csv"
        moved.parent.mkdir()
        shutil.move(f, moved)
        assert DataFileStep(_config(tmp, data_file=str(moved))).config_hash() == before


def test_config_hash_changes_when_a_file_of_a_list_parameter_is_edited():
    """Regression: `ListParameter(inner=PathType)` (i.e. `gtfs.files`) was not tracked at all."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        a = _write(tmp / "a.zip", "a\n")
        b = _write(tmp / "b.zip", "b\n")
        files = [str(a), str(b)]
        before = DataFilesStep(_config(tmp, data_files=files)).config_hash()
        _write(b, "modified\n")
        assert DataFilesStep(_config(tmp, data_files=files)).config_hash() != before


def test_config_hash_changes_when_a_file_inside_a_data_directory_is_edited():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        d = tmp / "dir"
        _write(d / "a.csv", "a\n")
        before = DataDirStep(_config(tmp, data_dir=str(d))).config_hash()
        _write(d / "a.csv", "modified\n")
        assert DataDirStep(_config(tmp, data_dir=str(d))).config_hash() != before


def test_config_hash_ignores_the_content_of_an_exec_path_parameter():
    """Switching Metropolis-Core version or OS must still not re-run every step."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        exe = _write(tmp / "metropolis_cli", "#!/bin/sh\n")
        before = ExecStep(_config(tmp, exec_path=str(exe))).config_hash()
        _write(exe, "#!/bin/sh\n# a new version\n")
        assert ExecStep(_config(tmp, exec_path=str(exe))).config_hash() == before


def test_missing_data_file_is_distinguished_from_an_unset_parameter():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        missing = tmp / "nope.csv"
        unset = DataFileStep(_config(tmp)).config_hash()
        absent = DataFileStep(_config(tmp, data_file=str(missing))).config_hash()
        assert unset != absent
        # Creating the file invalidates the step.
        _write(missing, "x,y\n")
        assert DataFileStep(_config(tmp, data_file=str(missing))).config_hash() != absent


# ---------------------------------------------------------------------------
# End-to-end through MetroPipeline
# ---------------------------------------------------------------------------


class OutFile(MetroTxtFile):
    path = "out.txt"


class WritingStep(Step):
    output_files = {"out": OutFile}
    data_file = PathParameter("dep.data_file")
    label = StringParameter("dep.label")

    def run(self):
        self.output["out"].write("done")


def _status(config: Config) -> StepStatus:
    (_, status) = MetroPipeline(config, [WritingStep]).find_sequence()[0]
    return status


def test_step_is_rerun_after_a_content_change_but_not_after_a_touch():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        f = _write(tmp / "data.csv", "x,y\n")
        config = _config(tmp, data_file=str(f), label="a")
        MetroPipeline(config, [WritingStep]).run()
        assert _status(_config(tmp, data_file=str(f), label="a")) == StepStatus.UP_TO_DATE

        # Touching the file changes its mtime but not its content: nothing to redo.
        f.touch()
        assert _status(_config(tmp, data_file=str(f), label="a")) == StepStatus.UP_TO_DATE

        # Editing it in place does require a re-run.
        _write(f, "x,y,z\n")
        assert _status(_config(tmp, data_file=str(f), label="a")) == StepStatus.OUTDATED


def test_update_required_does_not_raise_for_a_missing_data_file():
    """Regression: the removed mtime check called `stat()` unguarded on a set-but-absent path.

    Reachable through `survey.mobisurvstd.path`, the only path parameter declaring neither
    `check_file_exists` nor `check_dir_exists`: `save_update_dict` skipped the missing file, so
    the next run found no recorded mtime, fell through the guard and raised `FileNotFoundError`.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        missing = tmp / "nope.csv"
        config = _config(tmp, data_file=str(missing), label="a")
        MetroPipeline(config, [WritingStep]).run()
        step = WritingStep(_config(tmp, data_file=str(missing), label="a"))
        assert step.update_required() is False


def test_digest_cache_is_saved_by_the_pipeline():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        f = _write(tmp / "data.csv", "x,y\n")
        config = _config(tmp, data_file=str(f), label="a")
        MetroPipeline(config, [WritingStep])
        cache_path = config.main_directory / "update_files" / "digest_cache.json"
        assert cache_path.is_file()
        assert str(f) in json.loads(cache_path.read_text())
