from .capacities import CAPACITIES_FILES, CAPACITIES_STEPS
from .files import TargetRoutesFile, TomTomRoutesFile
from .free_flow import FREE_FLOW_FILES, FREE_FLOW_STEPS
from .map_matching import MapMatchingStep
from .tomtom import TomTomRequestsStep

ROAD_FILES = [TomTomRoutesFile, TargetRoutesFile, *FREE_FLOW_FILES, *CAPACITIES_FILES]

ROAD_STEPS = [TomTomRequestsStep, MapMatchingStep, *FREE_FLOW_STEPS, *CAPACITIES_STEPS]
