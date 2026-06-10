from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import IntParameter


class ThreadedStep(Step):
    """An abstract step with a configured number of threads to run tasks in parallel."""

    nb_threads = IntParameter(
        "nb_threads",
        description="Maximum number of threads to be used when running tasks in parallel.",
        note="Default is to use all the available threads.",
    )
