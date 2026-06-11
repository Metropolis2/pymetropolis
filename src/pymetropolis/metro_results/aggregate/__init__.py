from .files import AggregateOutputFile, IterationResultsFile
from .postprocess import AggregateResultsStep, IterationResultsStep

AGGREGATE_RESULTS_FILES = [IterationResultsFile, AggregateOutputFile]
AGGREGATE_RESULTS_STEPS = [IterationResultsStep, AggregateResultsStep]
