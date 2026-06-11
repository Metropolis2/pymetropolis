from .files import RouteResultsFile, TripResultsFile
from .postprocess import RouteResultsStep, TripResultsStep

DEMAND_RESULTS_FILES = [TripResultsFile, RouteResultsFile]
DEMAND_RESULTS_STEPS = [TripResultsStep, RouteResultsStep]
