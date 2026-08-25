from .files import (
    PrimaryCarTripsAccessEgressFile,
    TripsBicycleCostsFile,
    TripsBicycleNodesFile,
    TripsCarFreeFlowTravelTimesFile,
    TripsPedestrianDistancesFile,
    TripsPedestrianNodesFile,
    TripsPublicTransitItinerariesFile,
    TripsRoadNodesFile,
    ZonesLevel1RoadNodeFile,
    ZonesLevel2RoadNodeFile,
    ZonesLevel3RoadNodeFile,
    ZonesLevel4RoadNodeFile,
    ZonesLevel5RoadNodeFile,
)
from .od_pairs import (
    BicycleODNodesFromCoordinatesStep,
    PedestrianODNodesFromCoordinatesStep,
    RoadODNodesFromCoordinatesStep,
)
from .od_zones import (
    ZonesLevel1RoadNodesStep,
    ZonesLevel2RoadNodesStep,
    ZonesLevel3RoadNodesStep,
    ZonesLevel4RoadNodesStep,
    ZonesLevel5RoadNodesStep,
)
from .opentripplanner import TripsOpenTripPlannerStep
from .r5 import TripsPublicTransitTravelTimeFromR5Step
from .road_split import CarAccessEgressStep, RoadNetworkPrimaryEdgesStep
from .routing_cli import (
    TripsBicycleCostStep,
    TripsCarFreeFlowTravelTimesStep,
    TripsPedestrianDistancesStep,
)

ROUTING_FILES = [
    TripsPedestrianNodesFile,
    TripsPedestrianDistancesFile,
    TripsBicycleNodesFile,
    TripsBicycleCostsFile,
    TripsRoadNodesFile,
    TripsCarFreeFlowTravelTimesFile,
    PrimaryCarTripsAccessEgressFile,
    TripsPublicTransitItinerariesFile,
    ZonesLevel1RoadNodeFile,
    ZonesLevel2RoadNodeFile,
    ZonesLevel3RoadNodeFile,
    ZonesLevel4RoadNodeFile,
    ZonesLevel5RoadNodeFile,
]

ROUTING_STEPS = [
    PedestrianODNodesFromCoordinatesStep,
    BicycleODNodesFromCoordinatesStep,
    RoadODNodesFromCoordinatesStep,
    TripsPedestrianDistancesStep,
    TripsBicycleCostStep,
    TripsCarFreeFlowTravelTimesStep,
    RoadNetworkPrimaryEdgesStep,
    CarAccessEgressStep,
    TripsOpenTripPlannerStep,
    TripsPublicTransitTravelTimeFromR5Step,
    ZonesLevel1RoadNodesStep,
    ZonesLevel2RoadNodesStep,
    ZonesLevel3RoadNodesStep,
    ZonesLevel4RoadNodesStep,
    ZonesLevel5RoadNodesStep,
]
