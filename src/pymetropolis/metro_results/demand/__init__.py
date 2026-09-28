from .files import ActivityResultsFile, RouteResultsFile, TourResultsFile, TripResultsFile
from .postprocess import ActivityResultsStep, RouteResultsStep, TourResultsStep, TripResultsStep

DEMAND_RESULTS_FILES = [TourResultsFile, TripResultsFile, RouteResultsFile, ActivityResultsFile]
DEMAND_RESULTS_STEPS = [TourResultsStep, TripResultsStep, RouteResultsStep, ActivityResultsStep]
