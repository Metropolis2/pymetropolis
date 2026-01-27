from .metro_demand import FILES as METRO_DEMAND_FILES
from .metro_demand import STEPS as METRO_DEMAND_STEPS
from .metro_network import FILES as METRO_NETWORK_FILES
from .metro_network import STEPS as METRO_NETWORK_STEPS
from .metro_pipeline import MetroFile, Step
from .metro_results import FILES as METRO_RESULTS_FILES
from .metro_results import STEPS as METRO_RESULTS_STEPS
from .metro_simulation import FILES as METRO_SIMULATION_FILES
from .metro_simulation import STEPS as METRO_SIMULATION_STEPS
from .metro_spatial import FILES as METRO_SPATIAL_FILES
from .metro_spatial import STEPS as METRO_SPATIAL_STEPS

STEPS: list[type[Step]] = (
    METRO_SPATIAL_STEPS
    + METRO_NETWORK_STEPS
    + METRO_DEMAND_STEPS
    + METRO_SIMULATION_STEPS
    + METRO_RESULTS_STEPS
)

FILES: list[type[MetroFile]] = (
    METRO_SPATIAL_FILES
    + METRO_NETWORK_FILES
    + METRO_DEMAND_FILES
    + METRO_SIMULATION_FILES
    + METRO_RESULTS_FILES
)
