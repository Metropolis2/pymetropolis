import tempfile

from pymetropolis.metro_pipeline import Config, MetroFile, Step
from pymetropolis.metro_pipeline.pipeline import MetroPipeline
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
