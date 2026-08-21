import tempfile
from pathlib import Path

from pymetropolis.metro_pipeline import Config, MetroFile, Step
from pymetropolis.metro_pipeline.file import MetroTxtFile
from pymetropolis.metro_pipeline.parameters import StringParameter
from pymetropolis.metro_pipeline.pipeline import MetroPipeline, StepStatus
from pymetropolis.metro_pipeline.steps import InputFile


class File1(MetroFile):
    path = "file1"


class File2(MetroFile):
    path = "file2"


class File3(MetroFile):
    path = "file3"


class File4(MetroFile):
    path = "file4"


class A(Step):
    output_files = {"1": File1}


class B(Step):
    output_files = {"2": File2}


class C(Step):
    input_files = {"1": File1, "2": File2}
    output_files = {"3": File3}


class Cbis(Step):
    input_files = {"1": File1, "2": InputFile(File2, optional=True)}
    output_files = {"3": File3}


class Cter(Step):
    input_files = {"1": File1, "2": InputFile(File2, optional=True)}
    output_files = {"3": File3, "4": File4}


class D(Step):
    input_files = {"1": File1}
    output_files = {"3": File3}


def test_basic_pipeline():
    """Basic pipeline with 3 steps:

    - A generates 1
    - B generates 2
    - C reads 1 and 2 to generate 3
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config({"main_directory": tmp_dir})
        pipeline = MetroPipeline(config, [A, B, C])
        sequence = pipeline.find_sequence()
        assert len(sequence) == 3
        step_sequence = list(map(lambda x: x[0].__class__.__name__, sequence))
        assert step_sequence == ["A", "B", "C"] or step_sequence == ["B", "A", "C"]


def test_pipeline_with_optional():
    """Pipeline with an optional file read:

    - A generates 1
    - B generates 2
    - Cbis reads 1 and (optionally) 2 to generate 3
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config({"main_directory": tmp_dir})
        pipeline = MetroPipeline(config, [A, B, Cbis])
        sequence = pipeline.find_sequence()
        assert len(sequence) == 3
        step_sequence = list(map(lambda x: x[0].__class__.__name__, sequence))
        assert step_sequence == ["A", "B", "Cbis"] or step_sequence == ["B", "A", "Cbis"]


def test_pipeline_with_conflict():
    """Pipeline with a conflict to generate one file:

    - A generates 1
    - B generates 2
    - Cter reads 1 and (optionally) 2 to generate 3 and 4
    - D reads 1 to generate 3

    Cter generate more output files so it should be prefered over D
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config({"main_directory": tmp_dir})
        pipeline = MetroPipeline(config, [A, B, Cter, D])
        sequence = pipeline.find_sequence()
        assert len(sequence) == 3
        step_sequence = list(map(lambda x: x[0].__class__.__name__, sequence))
        assert step_sequence == ["A", "B", "Cter"] or step_sequence == ["B", "A", "Cter"]


def test_pipeline_with_optional_and_no_producer():
    """Pipeline with an optional file read whose producer is not part of the pipeline at all:

    - A generates 1
    - Cbis reads 1 and (optionally) 2 to generate 3, but no Step produces 2

    Cbis must still be scheduled right after A: an optional input that no Step in the pipeline
    can ever produce should not be waited on.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config({"main_directory": tmp_dir})
        pipeline = MetroPipeline(config, [A, Cbis])
        sequence = pipeline.find_sequence()
        step_sequence = list(map(lambda x: x[0].__class__.__name__, sequence))
        assert step_sequence == ["A", "Cbis"]


class PriorityWinner(Step):
    priority = 10
    input_files = {"1": File1}
    output_files = {"3": File3}


class ZPriorityLoser(Step):
    priority = 1
    input_files = {"1": File1}
    output_files = {"3": File3}


def test_pipeline_with_priority_conflict():
    """Pipeline with a conflict resolved by explicit Step priority, rather than by output count or
    alphabetical class name:

    - A generates 1
    - PriorityWinner (priority=10) and ZPriorityLoser (priority=1) both read 1 to generate 3

    Both competing Steps produce the same number of output files, and `ZPriorityLoser` sorts
    after `PriorityWinner` alphabetically, so PriorityWinner can only win the conflict if
    `priority` (the first criterion in `Step.__lt__`) is actually taken into account.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config({"main_directory": tmp_dir})
        pipeline = MetroPipeline(config, [A, PriorityWinner, ZPriorityLoser])
        sequence = pipeline.find_sequence()
        step_sequence = list(map(lambda x: x[0].__class__.__name__, sequence))
        assert step_sequence == ["A", "PriorityWinner"]


class FConflictA(MetroFile):
    path = "conflict_a"


class FConflictD(MetroFile):
    path = "conflict_d"


class FConflictE(MetroFile):
    path = "conflict_e"


class FConflictG(MetroFile):
    path = "conflict_g"


class FConflictH(MetroFile):
    path = "conflict_h"


class StepP(Step):
    """Loses the conflict on `FConflictA` (fewer outputs than StepQ)."""

    output_files = {"a": FConflictA, "d": FConflictD}


class StepQ(Step):
    """Wins the conflict on `FConflictA` (more outputs than StepP)."""

    output_files = {"a": FConflictA, "e": FConflictE, "g": FConflictG}


class StepM(Step):
    """Depends on `FConflictD`, which only StepP produces."""

    input_files = {"d": FConflictD}
    output_files = {"h": FConflictH}


def test_pipeline_with_cascading_infeasibility():
    """When a Step is removed to resolve a conflict, any other Step that depended on one of its
    outputs must also be dropped, not just the conflict loser itself:

    - StepP and StepQ both produce `FConflictA`; StepQ wins because it has more outputs
    - StepP alone produces `FConflictD`
    - StepM requires `FConflictD`, so once StepP is removed, StepM is no longer feasible
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        config = Config({"main_directory": tmp_dir})
        pipeline = MetroPipeline(config, [StepP, StepQ, StepM])
        sequence = pipeline.find_sequence()
        step_sequence = list(map(lambda x: x[0].__class__.__name__, sequence))
        assert step_sequence == ["StepQ"]


class TxtFile1(MetroTxtFile):
    path = "file1.txt"


class TxtFile2(MetroTxtFile):
    path = "file2.txt"


class TxtFile3(MetroTxtFile):
    path = "file3.txt"


class StepA(Step):
    """Writes `value` to file 1."""

    value = StringParameter("step_a.value", default="a")
    output_files = {"1": TxtFile1}

    def run(self):
        self.output["1"].write(self.value)


class StepB(Step):
    """Writes a constant to file 2."""

    output_files = {"2": TxtFile2}

    def run(self):
        self.output["2"].write("b")


class StepC(Step):
    """Reads files 1 and 2, writes their concatenation to file 3."""

    input_files = {"1": TxtFile1, "2": TxtFile2}
    output_files = {"3": TxtFile3}

    def run(self):
        self.output["3"].write(self.input["1"].read() + self.input["2"].read())


def _statuses_by_name(sequence: list) -> dict[str, StepStatus]:
    return {step.__class__.__name__: status for step, status in sequence}


def test_step_status_up_to_date():
    """Running the same pipeline twice with an unchanged config should mark every Step as
    UP_TO_DATE on the second run.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        config_dict = {"main_directory": tmp_dir, "step_a": {"value": "a"}}

        pipeline = MetroPipeline(Config(config_dict), [StepA, StepB, StepC])
        sequence = pipeline.find_sequence()
        assert _statuses_by_name(sequence) == {
            "StepA": StepStatus.OUTDATED,
            "StepB": StepStatus.OUTDATED,
            "StepC": StepStatus.OUTDATED,
        }
        pipeline.run_sequence(sequence)

        pipeline2 = MetroPipeline(Config(config_dict), [StepA, StepB, StepC])
        sequence2 = pipeline2.find_sequence()
        assert _statuses_by_name(sequence2) == {
            "StepA": StepStatus.UP_TO_DATE,
            "StepB": StepStatus.UP_TO_DATE,
            "StepC": StepStatus.UP_TO_DATE,
        }


def test_step_status_outdated_and_invalidated():
    """Changing the config value read by StepA should mark StepA as OUTDATED and StepC (which
    depends on StepA's output but did not change itself) as INVALIDATED, without StepB being
    affected.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        config_dict = {"main_directory": tmp_dir, "step_a": {"value": "a"}}
        pipeline = MetroPipeline(Config(config_dict), [StepA, StepB, StepC])
        pipeline.run_sequence(pipeline.find_sequence())

        new_config_dict = {"main_directory": tmp_dir, "step_a": {"value": "a2"}}
        pipeline2 = MetroPipeline(Config(new_config_dict), [StepA, StepB, StepC])
        sequence2 = pipeline2.find_sequence()
        assert _statuses_by_name(sequence2) == {
            "StepA": StepStatus.OUTDATED,
            "StepB": StepStatus.UP_TO_DATE,
            "StepC": StepStatus.INVALIDATED,
        }


def test_pipeline_with_custom_steps():
    """A Step defined in a `custom_steps` Python file is loaded and scheduled like any built-in
    Step.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        custom_steps_path = Path(tmp_dir) / "custom_steps.py"
        custom_steps_path.write_text(
            "from pymetropolis.metro_pipeline import MetroFile, Step\n"
            "\n"
            "class CustomFile(MetroFile):\n"
            "    path = 'custom_file'\n"
            "\n"
            "class CustomStep(Step):\n"
            "    output_files = {'1': CustomFile}\n"
        )
        config = Config({"main_directory": tmp_dir, "custom_steps": [str(custom_steps_path)]})
        pipeline = MetroPipeline(config, [])
        sequence = pipeline.find_sequence()
        step_sequence = list(map(lambda x: x[0].__class__.__name__, sequence))
        assert step_sequence == ["CustomStep"]


def test_pipeline_with_custom_steps_override():
    """A custom Step whose name matches an existing Step's name (e.g. a built-in one) replaces it
    in the pipeline, rather than raising an error, so that users can override a built-in Step
    (e.g. `EqasimImportStep`) with their own local-specific implementation.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        custom_steps_path = Path(tmp_dir) / "custom_steps.py"
        custom_steps_path.write_text(
            "from pymetropolis.metro_pipeline import MetroFile, Step\n"
            "\n"
            "class OverrideFile(MetroFile):\n"
            "    path = 'override_file'\n"
            "\n"
            "class A(Step):\n"
            "    overridden = True\n"
            "    output_files = {'1': OverrideFile}\n"
        )
        config = Config({"main_directory": tmp_dir, "custom_steps": [str(custom_steps_path)]})
        pipeline = MetroPipeline(config, [A])
        (overriding_step,) = pipeline.steps.keys()
        assert overriding_step.__class__.__name__ == "A"
        assert getattr(overriding_step.__class__, "overridden", False) is True


class IndepFileB(MetroFile):
    path = "indep_b"


class IndepFileC(MetroFile):
    path = "indep_c"


class IndepFileD(MetroFile):
    path = "indep_d"


class IndepFileE(MetroFile):
    path = "indep_e"


class IndepSinkFile(MetroFile):
    path = "indep_sink"


class IndepB(Step):
    output_files = {"b": IndepFileB}


class IndepC(Step):
    output_files = {"c": IndepFileC}


class IndepD(Step):
    output_files = {"d": IndepFileD}


class IndepE(Step):
    output_files = {"e": IndepFileE}


class IndepSink(Step):
    input_files = {"b": IndepFileB, "c": IndepFileC, "d": IndepFileD, "e": IndepFileE}
    output_files = {"sink": IndepSinkFile}


def test_pipeline_sequence_is_deterministic():
    """Steps that become runnable at the same time (here the four independent `Indep*` steps) are
    always sequenced in the same, name-sorted order.

    `Step` does not define `__hash__`, so iterating the `set` of remaining steps directly would
    order them by object id and yield a different sequence from one run to the next.
    """
    step_classes: list[type[Step]] = [IndepB, IndepC, IndepD, IndepE, IndepSink]
    expected = ["IndepB", "IndepC", "IndepD", "IndepE", "IndepSink"]
    permutations: list[list[type[Step]]] = [
        step_classes,
        list(reversed(step_classes)),
        [IndepSink, IndepD, IndepB, IndepE, IndepC],
        [IndepE, IndepC, IndepSink, IndepB, IndepD],
    ]
    # Feeding the step classes in different orders is what makes the instances' ids (and hence the
    # `set` iteration order) differ between the pipelines built below.
    for permutation in permutations:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config = Config({"main_directory": tmp_dir})
            pipeline = MetroPipeline(config, permutation)
            sequence = pipeline.find_sequence()
            step_sequence = [step.__class__.__name__ for step, _ in sequence]
            assert step_sequence == expected


def test_relative_main_directory_is_resolved_against_config_file():
    """A relative `main_directory` is resolved against the directory of the main config file, not
    against the current working directory.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        config_dir = Path(tmp_dir) / "subdir"
        config_dir.mkdir()
        config_path = config_dir / "config.toml"
        config_path.write_text('main_directory = "output"\n')
        config = Config.from_toml(config_path)
        assert config.main_directory == config_dir / "output"
        assert config.main_directory.is_dir()


def test_absolute_main_directory_is_unaffected_by_config_file_location():
    """An absolute `main_directory` is used as-is, regardless of the main config file's location."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        config_dir = Path(tmp_dir) / "subdir"
        config_dir.mkdir()
        config_path = config_dir / "config.toml"
        main_dir = Path(tmp_dir) / "output"
        config_path.write_text(f'main_directory = "{main_dir.as_posix()}"\n')
        config = Config.from_toml(config_path)
        assert config.main_directory == main_dir


def test_relative_secrets_file_is_resolved_against_config_file():
    """A relative `secrets_file` is resolved against the directory of the main config file, not
    against the current working directory.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        config_dir = Path(tmp_dir) / "subdir"
        config_dir.mkdir()
        (config_dir / "secrets.toml").write_text('mysecret = "hello"\n')
        config_path = config_dir / "config.toml"
        config_path.write_text('main_directory = "output"\nsecrets_file = "secrets.toml"\n')
        config = Config.from_toml(config_path)
        assert config.secrets == {"mysecret": "hello"}
