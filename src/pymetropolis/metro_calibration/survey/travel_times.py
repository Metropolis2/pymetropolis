from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_calibration.survey.files import (
    SurveyedTripsFile,
    SurveyedTripsPedestrianDistancesFile,
    SurveyedTripsPedestrianNodesFile,
    SurveyedTripsPublicTransitItinerariesFile,
)
from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_demand.routing.od_pairs import (
    StepWithPedestrianForbiddenTypes,
    identify_od_pairs,
)
from pymetropolis.metro_demand.routing.opentripplanner import OpenTripPlannerStep
from pymetropolis.metro_demand.routing.routing_cli import RoutingCLIStep, trip_routing
from pymetropolis.metro_network.pedestrian_network.files import PedestrianEdgesCleanFile
from pymetropolis.metro_pipeline.parameters import ListParameter
from pymetropolis.metro_pipeline.types import String

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

    forbidden_types = ListParameter(
        "pedestrian_network.forbidden_types",
        inner=String(),
        default=[],
        description=(
            "List of pedestrian edges' types that *cannot* be used as origin / destination edge."
        ),
        example='`["trunk", "trunk_link"]`',
    )
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

        # For trips departing from home, we find the best itinerary that arrives by the obsreved
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
            time=pl.time(hour=pl.col("minutes") // 60, minute=pl.col("minutes") % 60).dt.strftime(
                "%H:%M:%S"
            )
        )

        trips = trips.select("trip_id", *LNG_LAT_COLS, "time", "arrive_by")

        df = self.run_queries(trips)
        self.output["costs"].write(df)
