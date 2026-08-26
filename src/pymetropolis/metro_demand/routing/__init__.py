from .files import (
    NonPrimaryCarTrips,
    NonPrimaryParkAndRideCarTrips,
    ParkAndRideRoadNodesFile,
    ParkAndRideTripsCarFreeFlowTravelTimesFile,
    ParkAndRideTripsPublicTransitItinerariesFile,
    PrimaryCarTripsAccessEgressFile,
    PrimaryParkAndRideCarTripsAccessEgressFile,
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
    ParkAndRideRoadNodesFromCoordinatesStep,
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
from .opentripplanner import ParkAndRideTripsOpenTripPlannerStep, TripsOpenTripPlannerStep
from .r5 import TripsPublicTransitTravelTimeFromR5Step
from .road_split import (
    CarAccessEgressStep,
    ParkAndRideCarAccessEgressStep,
    RoadNetworkPrimaryEdgesStep,
)
from .routing_cli import (
    ParkAndRideTripsCarFreeFlowTravelTimesStep,
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
    NonPrimaryCarTrips,
    TripsPublicTransitItinerariesFile,
    ParkAndRideRoadNodesFile,
    ParkAndRideTripsCarFreeFlowTravelTimesFile,
    PrimaryParkAndRideCarTripsAccessEgressFile,
    NonPrimaryParkAndRideCarTrips,
    ParkAndRideTripsPublicTransitItinerariesFile,
    NonPrimaryCarTrips,
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
    ParkAndRideCarAccessEgressStep,
    ParkAndRideRoadNodesFromCoordinatesStep,
    ParkAndRideTripsOpenTripPlannerStep,
    ParkAndRideTripsCarFreeFlowTravelTimesStep,
    ZonesLevel1RoadNodesStep,
    ZonesLevel2RoadNodesStep,
    ZonesLevel3RoadNodesStep,
    ZonesLevel4RoadNodesStep,
    ZonesLevel5RoadNodesStep,
]
