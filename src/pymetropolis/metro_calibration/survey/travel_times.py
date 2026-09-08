from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_demand.modes.bicycle import StepWithBicycleSpeed
from pymetropolis.metro_demand.modes.walking import StepWithWalkingSpeed
from pymetropolis.metro_demand.routing.od_pairs import (
    StepWithPedestrianForbiddenTypes,
    StepWithRoadForbiddenTypes,
    identify_od_pairs,
)
from pymetropolis.metro_demand.routing.opentripplanner import OpenTripPlannerStep
from pymetropolis.metro_demand.routing.routing_cli import RoutingCLIStep, trip_routing
from pymetropolis.metro_network.pedestrian_network.files import PedestrianEdgesCleanFile
from pymetropolis.metro_network.road_network.files import (
    RoadEdgesCleanFile,
    RoadEdgesFreeFlowTravelTimeFile,
)
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_simulation.common import StepWithModes
from pymetropolis.metro_simulation.run.files import MetroExAnteSimulatedTravelTimeFunctionsFile

from .files import (
    SurveyedToursFile,
    SurveyedToursTravelTimesFile,
    SurveyedTripsCarTravelTimesFile,
    SurveyedTripsFile,
    SurveyedTripsPedestrianDistancesFile,
    SurveyedTripsPedestrianNodesFile,
    SurveyedTripsPublicTransitItinerariesFile,
    SurveyedTripsRoadNodesFile,
)

if TYPE_CHECKING:
    import polars as pl

LNG_LAT_COLS = ["origin_lng", "origin_lat", "destination_lng", "destination_lat"]


def check_has_cols(df: pl.DataFrame, cols: list[str]):
    missing_cols = set(cols) - set(df.columns)
    if missing_cols:
        missing_cols_str = ", ".join(missing_cols)
        raise MetropyError(f"Missing columns in surveyed trips: {missing_cols_str}")


class SurveyedPedestrianODNodesFromCoordinatesStep(StepWithPedestrianForbiddenTypes):
    """Identifies nodes on the pedestrian network to be used as origins and destinations of the
    surveyed trips.

    For this Step to work, the surveyed trips must have `origin_lng`, `origin_lat`,
    `destination_lng`, and `destination_lat` columns.

    First, this Step finds the nearest edge to the origin / destination coordinates.
    Edges whose type is specified in the
    [`forbidden_types`](parameters.md#pedestrian_networkforbidden_types) parameter are excluded from
    that search.
    Then, the origin / destination node is either the source or target of that nearest edge,
    whichever is closer.
    """

    input_files = {"edges": PedestrianEdgesCleanFile, "trips": SurveyedTripsFile}
    output_files = {"ods": SurveyedTripsPedestrianNodesFile}
    priority = 0

    def run(self):
        import geopandas as gpd

        trips: pl.DataFrame = self.input["trips"].read()
        check_has_cols(trips, LNG_LAT_COLS)
        trips = trips.select("trip_id", *LNG_LAT_COLS).drop_nulls()

        edges = self.input["edges"].read()
        edges = edges.loc[
            ~edges["edge_type"].isin(self.forbidden_types),
            ["edge_id", "geometry", "source", "target"],
        ]

        origins = gpd.GeoDataFrame(
            {"trip_id": trips["trip_id"]},
            geometry=gpd.GeoSeries.from_xy(
                trips["origin_lng"], trips["origin_lat"], crs="EPSG:4326"
            ).to_crs(edges.crs),
        )
        destinations = gpd.GeoDataFrame(
            {"trip_id": trips["trip_id"]},
            geometry=gpd.GeoSeries.from_xy(
                trips["destination_lng"], trips["destination_lat"], crs="EPSG:4326"
            ).to_crs(edges.crs),
        )

        ods = identify_od_pairs(edges, origins, destinations)
        ods = ods.select(
            "trip_id",
            origin_pedestrian_node="origin_node",
            destination_pedestrian_node="destination_node",
        )
        self.output["ods"].write(ods)


class SurveyedTripsPedestrianDistancesStep(RoutingCLIStep):
    """Computes the surveyed trips' distance on the pedestrian network.

    The distance is defined as the length of the shortest path from origin to destination node on
    the pedestrian network.
    """

    input_files = {"od_pairs": SurveyedTripsPedestrianNodesFile, "edges": PedestrianEdgesCleanFile}
    output_files = {"distances": SurveyedTripsPedestrianDistancesFile}

    def run(self):
        import polars as pl

        assert self.exec_path is not None

        edges_gdf = self.input["edges"].read()
        edges = pl.from_pandas(edges_gdf.loc[:, ["edge_id", "source", "target", "length"]]).rename(
            {"length": "weight"}
        )
        od_pairs = self.input["od_pairs"].read()
        trips = od_pairs.select(
            "trip_id",
            origin_node="origin_pedestrian_node",
            destination_node="destination_pedestrian_node",
        )
        df = trip_routing(trips, edges, self.exec_path)
        df = df.select("trip_id", pedestrian_distance="value")
        self.output["distances"].write(df)


class SurveyedTripsOpenTripPlannerStep(OpenTripPlannerStep):
    """Computes the surveyed trips' travel time and generalized time by public transit with
    OpenTripPlanner.

    For this Step to work, the surveyed trips must have `origin_lng`, `origin_lat`,
    `destination_lng`, and `destination_lat` columns.

    Check the TripsOpenTripPlannerStep for additional details.
    """

    input_files = {"trips": SurveyedTripsFile}
    output_files = {"costs": SurveyedTripsPublicTransitItinerariesFile}

    def run(self):
        import polars as pl

        trips = self.input["trips"].read()
        check_has_cols(trips, ["departure_time", "arrival_time", *LNG_LAT_COLS])

        # For trips departing from home, we find the best itinerary that arrives by the observed
        # arrival time at destination.
        # For trips departing with another activity purpose, we find the best itinerary that depart
        # at the observed departure time from origin.
        if "origin_purpose_group" not in trips.columns:
            trips = trips.with_columns(origin_purpose_group=None)
        trips = trips.with_columns(arrive_by=pl.col("origin_purpose_group").eq("home"))
        trips = trips.with_columns(
            minutes=pl.when("arrive_by").then("arrival_time").otherwise("departure_time")
        )
        # Convert time column to a HH:MM:SS string.
        trips = trips.with_columns(
            time=pl.time(
                hour=pl.col("minutes") // 60 % 24, minute=pl.col("minutes") % 60
            ).dt.strftime("%H:%M:%S")
        )

        trips = trips.select("trip_id", *LNG_LAT_COLS, "time", "arrive_by")

        # Drop trips with null values.
        trips = trips.drop_nulls()

        df = self.run_queries(trips)
        self.output["costs"].write(df)


class SurveyedRoadODNodesFromCoordinatesStep(StepWithRoadForbiddenTypes):
    """Identifies nodes on the road network to be used as origins and destinations of the surveyed
    trips.

    For this Step to work, the surveyed trips must have `origin_lng`, `origin_lat`,
    `destination_lng`, and `destination_lat` columns.

    First, this Step finds the nearest edge to the origin / destination coordinates.
    Edges whose type is specified in the
    [`forbidden_types`](parameters.md#road_networkforbidden_types) parameter are excluded from
    that search.
    Then, the origin / destination node is either the source or target of that nearest edge,
    whichever is closer.
    """

    input_files = {"edges": RoadEdgesCleanFile, "trips": SurveyedTripsFile}
    output_files = {"ods": SurveyedTripsRoadNodesFile}
    priority = 0

    def run(self):
        import geopandas as gpd

        trips: pl.DataFrame = self.input["trips"].read()
        check_has_cols(trips, LNG_LAT_COLS)
        trips = trips.select("trip_id", *LNG_LAT_COLS).drop_nulls()

        edges = self.input["edges"].read()
        edges = edges.loc[
            ~edges["edge_type"].isin(self.forbidden_types),
            ["edge_id", "geometry", "source", "target"],
        ]

        origins = gpd.GeoDataFrame(
            {"trip_id": trips["trip_id"]},
            geometry=gpd.GeoSeries.from_xy(
                trips["origin_lng"], trips["origin_lat"], crs="EPSG:4326"
            ).to_crs(edges.crs),
        )
        destinations = gpd.GeoDataFrame(
            {"trip_id": trips["trip_id"]},
            geometry=gpd.GeoSeries.from_xy(
                trips["destination_lng"], trips["destination_lat"], crs="EPSG:4326"
            ).to_crs(edges.crs),
        )

        ods = identify_od_pairs(edges, origins, destinations)
        ods = ods.select(
            "trip_id", origin_road_node="origin_node", destination_road_node="destination_node"
        )
        self.output["ods"].write(ods)


class SurveyedTripsCarTravelTimesStep(RoutingCLIStep):
    """Computes the surveyed trips' travel time on the road network by car, under congested
    conditions.

    Congested conditions are read from the ex-ante simulation.
    """

    input_files = {
        "trips": SurveyedTripsFile,
        "od_pairs": SurveyedTripsRoadNodesFile,
        "edges": RoadEdgesCleanFile,
        "edges_fftt": RoadEdgesFreeFlowTravelTimeFile,
        "congestion_conditions": MetroExAnteSimulatedTravelTimeFunctionsFile,
    }
    output_files = {"tt": SurveyedTripsCarTravelTimesFile}

    def run(self):
        import polars as pl

        assert self.exec_path is not None

        edges_gdf = self.input["edges"].read()
        edges_ttfs = self.input["congestion_conditions"].read()
        # Add free-flow travel time to edges' weights so that it's use as default for edges not in
        # the congested conditions.
        edges_fftt = self.input["edges_fftt"].read()
        edges = (
            pl.from_pandas(edges_gdf.loc[:, ["edge_id", "source", "target", "length"]])
            .join(edges_fftt, on="edge_id", how="left")
            .with_columns(pl.col("free_flow_travel_time").dt.total_nanoseconds() / 1e9)
            .rename({"free_flow_travel_time": "weight"})
        )
        n = edges["weight"].null_count()
        if n:
            logger.warning(f"Discarding {n} edges with NULL free-flow travel time")
            edges = edges.filter(pl.col("weight").is_not_null())

        trips = self.input["trips"].read()
        check_has_cols(trips, ["departure_time"])
        od_pairs = self.input["od_pairs"].read()
        trips = trips.join(od_pairs, on="trip_id", how="left").select(
            "trip_id",
            pl.duration(minutes="departure_time"),
            origin_node="origin_road_node",
            destination_node="destination_road_node",
        )
        df = trip_routing(
            trips, edges, self.exec_path, with_routes=False, network_conditions=edges_ttfs
        )
        df = df.select("trip_id", car_travel_time=pl.duration(seconds="value"))
        self.output["tt"].write(df)


class SurveyedToursTravelTimesStep(StepWithModes, StepWithWalkingSpeed, StepWithBicycleSpeed):
    """Computes the travel times for surveyed tours for all modes."""

    input_files = {
        "tours": SurveyedToursFile,
        "pedestrian_distance": InputFile(
            SurveyedTripsPedestrianDistancesFile,
            when=lambda inst: inst.has_pedestrian_mode(),
            when_doc='if the "walking" or "bicycle" mode is defined',
        ),
        "pt_tt": InputFile(
            SurveyedTripsPublicTransitItinerariesFile,
            when=lambda inst: inst.has_mode("public_transit"),
            when_doc='if the "public_transit" mode is defined',
        ),
        "car_tt": InputFile(
            SurveyedTripsCarTravelTimesFile,
            when=lambda inst: inst.has_car_mode(),
            when_doc=r'if any "car\_\*" mode is defined',
        ),
    }
    output_files = {"tt": SurveyedToursTravelTimesFile}

    def is_defined(self):
        return (
            self.has_trip_mode()
            and (not self.has_mode("walking") or self.walking_speed is not None)
            and (not self.has_mode("bicycle") or self.bicycle_speed is not None)
        )

    def run(self):
        import polars as pl

        tours = self.input["tours"].read().select("tour_id", "trip_ids")
        if self.has_pedestrian_mode():
            dists = self.input["pedestrian_distance"].read()
            tours = tours.with_columns(
                pedestrian_distance=pl.col("trip_ids")
                .list.eval(
                    pl.element().replace_strict(
                        dists["trip_id"], dists["pedestrian_distance"], default=None
                    )
                )
                .list.sum()
            )
            if self.has_mode("walking"):
                assert self.walking_speed is not None
                tours = tours.with_columns(
                    travel_time_walking=pl.duration(
                        hours=(pl.col("pedestrian_distance") / 1000) / self.walking_speed
                    )
                )
            if self.has_mode("bicycle"):
                assert self.bicycle_speed is not None
                tours = tours.with_columns(
                    travel_time_bicycle=pl.duration(
                        hours=(pl.col("pedestrian_distance") / 1000) / self.bicycle_speed
                    )
                )
            tours = tours.drop("pedestrian_distance")
        if self.has_mode("public_transit"):
            pt_tt = self.input["pt_tt"].read()
            tours = tours.with_columns(
                travel_time_public_transit=pl.col("trip_ids")
                .list.eval(
                    pl.element().replace_strict(
                        pt_tt["trip_id"], pt_tt["generalized_time"], default=None
                    )
                )
                .list.sum()
            )
        if self.has_car_mode():
            car_tt = self.input["car_tt"].read()
            tours = tours.with_columns(
                travel_time_car=pl.col("trip_ids")
                .list.eval(
                    pl.element().replace_strict(
                        car_tt["trip_id"], car_tt["travel_time"], default=None
                    )
                )
                .list.sum()
            )
        tours = tours.drop("trip_id")
        self.output["tt"].write(tours)
