from .files import (
    PrimaryCarTripsAccessEgressFile,
    # TripsBicycleCostsFile,
    # TripsBicycleNodesFile,
    TripsCarFreeFlowTravelTimesFile,
    TripsPedestrianDistancesFile,
    TripsPedestrianNodesFile,
    TripsRoadNodesFile,
)
from .od_pairs import (
    # BicycleODNodesFromCoordinatesStep,
    PedestrianODNodesFromCoordinatesStep,
    RoadODNodesFromCoordinatesStep,
)
from .road_split import CarAccessEgressStep, RoadNetworkPrimaryEdgesStep
from .routing_cli import (
    # TripsBicycleCostStep,
    TripsCarFreeFlowTravelTimesStep,
    TripsPedestrianDistancesStep,
)

ROUTING_FILES = [
    TripsPedestrianNodesFile,
    TripsPedestrianDistancesFile,
    # TripsBicycleNodesFile,
    # TripsBicycleCostsFile,
    TripsRoadNodesFile,
    TripsCarFreeFlowTravelTimesFile,
    PrimaryCarTripsAccessEgressFile,
]

ROUTING_STEPS = [
    PedestrianODNodesFromCoordinatesStep,
    # BicycleODNodesFromCoordinatesStep,
    RoadODNodesFromCoordinatesStep,
    TripsPedestrianDistancesStep,
    # TripsBicycleCostStep,
    TripsCarFreeFlowTravelTimesStep,
    RoadNetworkPrimaryEdgesStep,
    CarAccessEgressStep,
]
