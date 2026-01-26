from .metro_demand import STEPS as METRO_DEMAND_STEPS
from .metro_network import STEPS as METRO_NETWORK_STEPS
from .metro_pipeline import Step
from .metro_results import STEPS as METRO_RESULTS_STEPS
from .metro_simulation import STEPS as METRO_SIMULATION_STEPS
from .metro_spatial import STEPS as METRO_SPATIAL_STEPS

STEPS: list[type[Step]] = (
    METRO_SPATIAL_STEPS
    + METRO_NETWORK_STEPS
    + METRO_DEMAND_STEPS
    + METRO_SIMULATION_STEPS
    + METRO_RESULTS_STEPS
)
