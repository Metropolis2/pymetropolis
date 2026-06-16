from .files import ActivityResultsFile, RouteResultsFile, TripResultsFile
from .postprocess import ActivityResultsStep, RouteResultsStep, TripResultsStep

DEMAND_RESULTS_FILES = [TripResultsFile, RouteResultsFile, ActivityResultsFile]
DEMAND_RESULTS_STEPS = [TripResultsStep, RouteResultsStep, ActivityResultsStep]
