from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType


class TripsPedestrianNodesFile(MetroDataFrameFile):
    path = "demand/population/trips/pedestrian/origins_destinations.parquet"
    description = "Origin and destination nodes on the pedestrian network for each trip."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "origin_pedestrian_node",
            MetroDataType.ID,
            description="Identifier of the origin node on the pedestrian network.",
            nullable=True,
        ),
        Column(
            "origin_pedestrian_node_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's origin and the corresponding pedestrian node, "
                "in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_pedestrian_node_dist_on_edge",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's origin and the corresponding pedestrian node, "
                "projected on the closest edge, in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_pedestrian_edge",
            MetroDataType.ID,
            description="Identifier of the pedestrian edge closest to the trip's origin.",
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_pedestrian_edge_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's origin and the closest pedestrian edge, in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_pedestrian_node",
            MetroDataType.ID,
            description="Identifier of the destination node on the pedestrian network.",
            nullable=True,
        ),
        Column(
            "destination_pedestrian_node_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's destination and the corresponding pedestrian node, "
                "in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_pedestrian_node_dist_on_edge",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's destination and the corresponding pedestrian node, "
                "projected on the closest edge, in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_pedestrian_edge",
            MetroDataType.ID,
            description="Identifier of the pedestrian edge closest to the trip's destination.",
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_pedestrian_edge_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's destination and the closest pedestrian edge, "
                "in meters."
            ),
            nullable=True,
            optional=True,
        ),
    ]


class TripsPedestrianDistancesFile(MetroDataFrameFile):
    path = "demand/population/trips/pedestrian/distances.parquet"
    description = "Distance of the shortest path on the pedestrian network for each trip."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "pedestrian_distance",
            MetroDataType.FLOAT,
            description="Distance of the trip on the pedestrian network, in meters.",
            nullable=True,
        ),
        Column(
            "pedestrian_path",
            MetroDataType.LIST_OF_IDS,
            description="Shortest path of the trip on the pedestrian network, as a list of ids.",
            nullable=True,
            optional=True,
        ),
    ]


class TripsBicycleNodesFile(MetroDataFrameFile):
    path = "demand/population/trips/bicycle/origins_destinations.parquet"
    description = "Origin and destination nodes on the bicycle network for each trip."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "origin_bicycle_node",
            MetroDataType.ID,
            description="Identifier of the origin node on the bicycle network.",
            nullable=True,
        ),
        Column(
            "origin_bicycle_node_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's origin and the corresponding bicycle node, in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_bicycle_node_dist_on_edge",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's origin and the corresponding bicycle node, "
                "projected on the closest edge, in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_bicycle_edge",
            MetroDataType.ID,
            description="Identifier of the bicycle edge closest to the trip's origin.",
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_bicycle_edge_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's origin and the closest bicycle edge, in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_bicycle_node",
            MetroDataType.ID,
            description="Identifier of the destination node on the bicycle network.",
            nullable=True,
        ),
        Column(
            "destination_bicycle_node_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's destination and the corresponding bicycle node, "
                "in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_bicycle_node_dist_on_edge",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's destination and the corresponding bicycle node, "
                "projected on the closest edge, in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_bicycle_edge",
            MetroDataType.ID,
            description="Identifier of the bicycle edge closest to the trip's destination.",
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_bicycle_edge_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's destination and the closest bicycle edge, in meters."
            ),
            nullable=True,
            optional=True,
        ),
    ]


class TripsBicycleCostsFile(MetroDataFrameFile):
    path = "demand/population/trips/bicycle/costs.parquet"
    description = "Minimum cost on the bicycle network for each trip."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "bicycle_cost",
            MetroDataType.FLOAT,
            description="Minimum cost of the trip on the bicycle network, in meters.",
            nullable=True,
        ),
        Column(
            "bicycle_path",
            MetroDataType.LIST_OF_IDS,
            description="Minimum-cost path of the trip on the bicycle network, as a list of ids.",
            nullable=True,
        ),
    ]


class TripsRoadNodesFile(MetroDataFrameFile):
    path = "demand/population/trips/road/origins_destinations.parquet"
    description = "Origin and destination nodes on the road network for each trip."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "origin_road_node",
            MetroDataType.ID,
            description="Identifier of the origin node on the road network.",
            nullable=True,
        ),
        Column(
            "origin_road_node_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's origin and the corresponding road node, in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_road_node_dist_on_edge",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's origin and the corresponding road node, "
                "projected on the closest edge, in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_road_edge",
            MetroDataType.ID,
            description="Identifier of the road edge closest to the trip's origin.",
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_road_edge_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's origin and the closest road edge, in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_road_node",
            MetroDataType.ID,
            description="Identifier of the destination node on the road network.",
            nullable=True,
        ),
        Column(
            "destination_road_node_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's destination and the corresponding road node, "
                "in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_road_node_dist_on_edge",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's destination and the corresponding road node, "
                "projected on the closest edge, in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_road_edge",
            MetroDataType.ID,
            description="Identifier of the road edge closest to the trip's destination.",
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_road_edge_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's destination and the closest road edge, in meters."
            ),
            nullable=True,
            optional=True,
        ),
    ]


class ParkAndRideRoadNodesFile(MetroDataFrameFile):
    path = "demand/population/trips/park_and_ride/road_nodes.parquet"
    description = "Road nodes matching the park-and-ride facility of each tour."
    schema = [
        Column(
            "tour_id",
            MetroDataType.ID,
            description="Identifier of the tour.",
            unique=True,
            nullable=False,
        ),
        Column(
            "pr_road_node",
            MetroDataType.ID,
            description="Identifier of the P+R node on the road network.",
            nullable=True,
        ),
        Column(
            "pr_road_node_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the tour's P+R facility and the corresponding road node, "
                "in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "pr_road_node_dist_on_edge",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's P+R facility and the corresponding road node, "
                "projected on the closest edge, in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "pr_road_edge",
            MetroDataType.ID,
            description="Identifier of the road edge closest to the trip's P+R facility.",
            nullable=True,
            optional=True,
        ),
        Column(
            "pr_road_edge_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's P+R facility and the closest road edge, in meters."
            ),
            nullable=True,
            optional=True,
        ),
    ]


class TripsCarFreeFlowTravelTimesFile(MetroDataFrameFile):
    path = "demand/population/trips/road/free_flow_travel_times.parquet"
    description = "Travel time by car under free-flow conditions for each trip."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "free_flow_travel_time",
            MetroDataType.DURATION,
            description="Travel time by car under free-flow conditions.",
            nullable=True,
        ),
        Column(
            "free_flow_route",
            MetroDataType.LIST_OF_IDS,
            description=(
                "Fastest path on the road network under free-flow conditions, as a list of ids."
            ),
            nullable=True,
        ),
        Column(
            "free_flow_distance",
            MetroDataType.FLOAT,
            description=(
                "Length of the fastest path on the road network under free-flow conditions, "
                "in meters."
            ),
            nullable=True,
        ),
    ]


class ParkAndRideTripsCarFreeFlowTravelTimesFile(MetroDataFrameFile):
    path = "demand/population/trips/park_and_ride/road_free_flow_travel_times.parquet"
    description = "Travel time under free-flow conditions of the car part for P+R trip."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip (only first and last trip of each tour).",
            unique=True,
            nullable=False,
        ),
        Column(
            "free_flow_travel_time",
            MetroDataType.DURATION,
            description=(
                "Travel time by car under free-flow conditions (from / to the P+R facility)."
            ),
            nullable=True,
        ),
        Column(
            "free_flow_route",
            MetroDataType.LIST_OF_IDS,
            description=(
                "Fastest path on the road network under free-flow conditions, as a list of ids."
            ),
            nullable=True,
        ),
        Column(
            "free_flow_distance",
            MetroDataType.FLOAT,
            description=(
                "Length of the fastest path on the road network under free-flow conditions, "
                "in meters."
            ),
            nullable=True,
        ),
    ]


class PrimaryCarTripsAccessEgressFile(MetroDataFrameFile):
    path = "demand/population/trips/road/primary_car_trips_access_egress.parquet"
    description = "Data on the access / egress parts of the car trips."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "access_node",
            MetroDataType.ID,
            description="Identifier of the road-network node where the primary part starts.",
            nullable=True,
        ),
        Column(
            "access_path",
            MetroDataType.LIST_OF_IDS,
            description="List of edge ids that consists the access part of the trip.",
            nullable=True,
        ),
        Column(
            "access_time",
            MetroDataType.DURATION,
            description=(
                "Time spent on the access part of the trip when traveling by car under free-flow "
                "conditions."
            ),
            nullable=True,
        ),
        Column(
            "access_length",
            MetroDataType.FLOAT,
            description="Length of the access part of the trip, in meters.",
            nullable=True,
        ),
        Column(
            "egress_node",
            MetroDataType.ID,
            description="Identifier of the road-network node where the primary part ends.",
            nullable=True,
        ),
        Column(
            "egress_path",
            MetroDataType.LIST_OF_IDS,
            description="List of edge ids that consists the egress part of the trip.",
            nullable=True,
        ),
        Column(
            "egress_time",
            MetroDataType.DURATION,
            description=(
                "Time spent on the egress part of the trip when traveling by car under free-flow "
                "conditions."
            ),
            nullable=True,
        ),
        Column(
            "egress_length",
            MetroDataType.FLOAT,
            description="Length of the egress part of the trip, in meters.",
            nullable=True,
        ),
    ]


class PrimaryParkAndRideCarTripsAccessEgressFile(MetroDataFrameFile):
    path = "demand/population/trips/park_and_ride/primary_car_trips_access_egress.parquet"
    description = "Data on access / egress of the car parts for park-and-ride trips."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "access_node",
            MetroDataType.ID,
            description="Identifier of the road-network node where the primary part starts.",
            nullable=True,
        ),
        Column(
            "access_path",
            MetroDataType.LIST_OF_IDS,
            description="List of edge ids that consists the access part of the trip.",
            nullable=True,
        ),
        Column(
            "access_time",
            MetroDataType.DURATION,
            description=(
                "Time spent on the access part of the trip when traveling by car under free-flow "
                "conditions."
            ),
            nullable=True,
        ),
        Column(
            "access_length",
            MetroDataType.FLOAT,
            description="Length of the access part of the trip, in meters.",
            nullable=True,
        ),
        Column(
            "egress_node",
            MetroDataType.ID,
            description="Identifier of the road-network node where the primary part ends.",
            nullable=True,
        ),
        Column(
            "egress_path",
            MetroDataType.LIST_OF_IDS,
            description="List of edge ids that consists the egress part of the trip.",
            nullable=True,
        ),
        Column(
            "egress_time",
            MetroDataType.DURATION,
            description=(
                "Time spent on the egress part of the trip when traveling by car under free-flow "
                "conditions."
            ),
            nullable=True,
        ),
        Column(
            "egress_length",
            MetroDataType.FLOAT,
            description="Length of the egress part of the trip, in meters.",
            nullable=True,
        ),
    ]


class NonPrimaryCarTrips(MetroDataFrameFile):
    path = "demand/population/trips/road/non_primary_car_trips.parquet"
    description = "Data on car trips traveling exclusively on non-primary edges."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "free_flow_travel_time",
            MetroDataType.DURATION,
            description="Travel time by car under free-flow conditions.",
            nullable=True,
        ),
        Column(
            "path",
            MetroDataType.LIST_OF_IDS,
            description="List of (non-primary) edge ids that consists the trip.",
            nullable=True,
        ),
        Column(
            "path_length",
            MetroDataType.FLOAT,
            description="Length of the trip, in meters.",
            nullable=True,
        ),
    ]


class NonPrimaryParkAndRideCarTrips(MetroDataFrameFile):
    path = "demand/population/trips/park_and_ride/non_primary_car_trips.parquet"
    description = (
        "Data on car parts of park-and-ride trips, when traveling exclusively on non-primary edges."
    )
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "free_flow_travel_time",
            MetroDataType.DURATION,
            description="Travel time by car under free-flow conditions.",
            nullable=True,
        ),
        Column(
            "path",
            MetroDataType.LIST_OF_IDS,
            description="List of (non-primary) edge ids that consists the trip.",
            nullable=True,
        ),
        Column(
            "path_length",
            MetroDataType.FLOAT,
            description="Length of the trip, in meters.",
            nullable=True,
        ),
    ]


class TripsPublicTransitItinerariesFile(MetroDataFrameFile):
    path = "demand/population/trips/public_transit/itineraries.parquet"
    description = "Minimum-cost public-transit itinerary for each trip."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "travel_time",
            MetroDataType.DURATION,
            description="Travel time of the trip.",
            nullable=True,
        ),
        Column(
            "generalized_time",
            MetroDataType.DURATION,
            description="Generalized time of the trip (travel time with mode-specific weights).",
            nullable=True,
            optional=True,
        ),
        Column(
            "waiting_time",
            MetroDataType.DURATION,
            description="Waiting time on the trip.",
            nullable=True,
            optional=True,
        ),
        Column(
            "legs",
            MetroDataType.ANY,
            description="Sequence of legs that define the itinerary of the trip.",
            nullable=True,
            optional=True,
        ),
    ]


class ParkAndRideTripsPublicTransitItinerariesFile(MetroDataFrameFile):
    path = "demand/population/trips/park_and_ride/public_transit_itineraries.parquet"
    description = (
        "Minimum-cost public-transit itinerary for the public-transit part of each P+R trip."
    )
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "travel_time",
            MetroDataType.DURATION,
            description="Travel time of the trip (public-transit part).",
            nullable=True,
        ),
        Column(
            "generalized_time",
            MetroDataType.DURATION,
            description="Generalized time of the trip (travel time with mode-specific weights).",
            nullable=True,
            optional=True,
        ),
        Column(
            "waiting_time",
            MetroDataType.DURATION,
            description="Waiting time on the trip.",
            nullable=True,
            optional=True,
        ),
        Column(
            "legs",
            MetroDataType.ANY,
            description="Sequence of legs that define the itinerary of the trip.",
            nullable=True,
            optional=True,
        ),
    ]
