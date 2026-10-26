from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.algorithms import find_kmedoids
from pymetropolis.metro_common.plots import PlotStep, plot_travel_time_comparison
from pymetropolis.metro_demand.modes.bicycle import StepWithBicycleSpeed
from pymetropolis.metro_demand.modes.walking import StepWithWalkingSpeed
from pymetropolis.metro_demand.routing.od_pairs import (
    StepWithPedestrianForbiddenTypes,
    StepWithRoadForbiddenTypes,
    create_source_target_points,
    identify_nodes,
    identify_od_pairs,
)
from pymetropolis.metro_demand.routing.opentripplanner import OpenTripPlannerStep
from pymetropolis.metro_demand.routing.routing_cli import RoutingCLIStep, trip_routing
from pymetropolis.metro_network.pedestrian_network.files import PedestrianEdgesCleanFile
from pymetropolis.metro_network.road_network.files import (
    RoadEdgesCleanFile,
    RoadEdgesFreeFlowTravelTimeFile,
)
from pymetropolis.metro_pipeline.parameters import EnumParameter, FloatParameter, IntParameter
from pymetropolis.metro_pipeline.steps import InputFile, Step
from pymetropolis.metro_simulation.run.files import MetroExAnteSimulatedTravelTimeFunctionsFile
from pymetropolis.modes import CarDriverAloneVehicle, StepWithModes
from pymetropolis.random import RandomStep

from .files import (
    SurveyedDetailedZonesFile,
    SurveyedSpecialLocationsFile,
    SurveyedToursFile,
    SurveyedToursTravelTimesFile,
    SurveyedTripsCarTravelTimesFile,
    SurveyedTripsDestinationsFile,
    SurveyedTripsFile,
    SurveyedTripsOriginsFile,
    SurveyedTripsPedestrianDistancesFile,
    SurveyedTripsPedestrianNodesFile,
    SurveyedTripsPublicTransitItinerariesFile,
    SurveyedTripsRoadNodesFile,
    SurveyedTripsTravelTimeComparisonBicyclePlotFile,
    SurveyedTripsTravelTimeComparisonCarPlotFile,
    SurveyedTripsTravelTimeComparisonPublicTransitPlotFile,
    SurveyedTripsTravelTimeComparisonWalkingPlotFile,
    SurveyedZonesMedoidsFile,
    SurveyedZonesPedestrianNodesFile,
    SurveyedZonesRoadNodesFile,
)

if TYPE_CHECKING:
    import geopandas as gpd
    import numpy as np
    import polars as pl


def check_has_cols(df: pl.DataFrame, cols: list[str]):
    missing_cols = set(cols) - set(df.columns)
    if missing_cols:
        missing_cols_str = ", ".join(missing_cols)
        raise MetropyError(f"Missing columns in surveyed trips: {missing_cols_str}")


class StepWithSurveyODMethod(Step):
    """Abstract Step to hold the `survey.od_method` parameter, shared by all the Steps computing
    travel times / distances of the surveyed trips, so that all modes use the same origins and
    destinations.
    """

    od_method = EnumParameter(
        "survey.od_method",
        values=["medoids", "distance_matching"],
        default="medoids",
        description=(
            "Method used to select the origin / destination of the surveyed trips when computing "
            "their travel times and distances, for all modes."
        ),
        note=(
            '`"medoids"` uses the zones\' medoids (all trips with the same origin and destination '
            'zones get the same values); `"distance_matching"` draws an origin and destination '
            "point for each trip that matches the trip's Euclidean distance (see "
            "`SurveyedTripsODPointsStep`)"
        ),
    )

    def uses_medoids(self) -> bool:
        return self.od_method == "medoids"

    def uses_distance_matching(self) -> bool:
        return self.od_method == "distance_matching"


def snap_survey_points(
    edges: gpd.GeoDataFrame,
    forbidden_types: list[str],
    origins: gpd.GeoDataFrame,
    destinations: gpd.GeoDataFrame,
) -> pl.DataFrame:
    """Returns the nearest network nodes to the origin / destination points of the surveyed trips
    (columns `trip_id`, `origin_node`, `destination_node`).
    """
    edges = edges.loc[
        ~edges["edge_type"].isin(forbidden_types), ["edge_id", "geometry", "source", "target"]
    ].copy()
    # Note. `identify_od_pairs` joins origins and destinations on `trip_id` (the order of the rows
    # does not matter).
    ods = identify_od_pairs(edges, origins.to_crs(edges.crs), destinations.to_crs(edges.crs))
    return ods.select("trip_id", "origin_node", "destination_node")


class SurveyedZoneMedoidsStep(StepWithPedestrianForbiddenTypes, RandomStep):
    """Runs a KMedoid clustering on the pedestrian nodes in each zone of the survey to origin /
    destination coordinates.
    """

    nb_clusters = IntParameter(
        "survey.nb_medoids",
        default=1,
        description="Number of clusters when running the KMedoid algorithm on survey zones.",
        note=(
            "Larger values increase the running time quadratically, but lead to more accurate "
            "travel times"
        ),
    )
    input_files = {
        "edges": PedestrianEdgesCleanFile,
        "trips": SurveyedTripsFile,
        "zones": SurveyedDetailedZonesFile,
    }
    output_files = {"medoids": SurveyedZonesMedoidsFile}
    priority = 0

    def run(self):
        import polars as pl

        assert self.nb_clusters is not None
        assert self.forbidden_types is not None

        zones = self.input["zones"].read()

        # Restrict zones to observed trips.
        trips = self.input["trips"].read()
        check_has_cols(trips, ["origin_detailed_zone", "destination_detailed_zone"])
        if "outside_perimeter" in trips.columns:
            trips = trips.filter(pl.col("outside_perimeter").not_())
        observed_zones = set(trips["origin_detailed_zone"]) | set(
            trips["destination_detailed_zone"]
        )
        zones = zones.loc[zones["detailed_zone_id"].isin(observed_zones)]
        if len(zones) == 0:
            raise MetropyError("No zone with observed trips (mismatched between zone ids?).")
        n = len(observed_zones)
        if n > len(zones):
            logger.warning(f"Origin / destination is not a valid zone for {n - len(zones)} ids.")
        zones["zone_id"] = zones["detailed_zone_id"]
        zones = zones[["zone_id", "geometry"]]

        edges: gpd.GeoDataFrame = self.input["edges"].read()
        edges = edges.loc[
            ~edges["edge_type"].isin(self.forbidden_types),
            ["edge_id", "geometry", "source", "target", "length"],
        ]

        medoids = find_kmedoids(edges, zones, self.nb_clusters, self.random_seed)
        self.output["medoids"].write(medoids)


class SurveyedPedestrianODNodesFromMedoidsStep(StepWithPedestrianForbiddenTypes):
    """Identifies nodes on the pedestrian network to be used as origin / destination for each
    surveyed zone from their medoids.
    """

    input_files = {"edges": PedestrianEdgesCleanFile, "medoids": SurveyedZonesMedoidsFile}
    output_files = {"nodes": SurveyedZonesPedestrianNodesFile}
    priority = 0

    def run(self):
        import polars as pl

        medoids = self.input["medoids"].read()
        medoids["medoid_id"] = medoids["zone_id"].astype(str) + "-" + medoids["index"].astype(str)

        edges = self.input["edges"].read()
        edges = edges.loc[
            ~edges["edge_type"].isin(self.forbidden_types),
            ["edge_id", "geometry", "source", "target"],
        ]

        edges = create_source_target_points(edges)
        nodes = identify_nodes(edges, medoids, "medoid_id")

        nodes = nodes.join(pl.from_pandas(medoids.drop(columns="geometry")), on="medoid_id").select(
            "zone_id", "index", "weight", "node"
        )
        self.output["nodes"].write(nodes)


class SurveyedTripsPedestrianDistancesStep(StepWithSurveyODMethod, RoutingCLIStep):
    """Computes the surveyed trips' distance on the pedestrian network.

    The distance is defined as the length of the shortest path from origin to destination node on
    the pedestrian network.
    The origin / destination nodes depend on the [`od_method`](parameters.md#surveyod_method)
    parameter.
    """

    input_files = {
        "nodes": InputFile(
            SurveyedZonesPedestrianNodesFile,
            when=lambda inst: inst.uses_medoids(),
            when_doc='if `od_method` is `"medoids"`',
        ),
        "trip_nodes": InputFile(
            SurveyedTripsPedestrianNodesFile,
            when=lambda inst: inst.uses_distance_matching(),
            when_doc='if `od_method` is `"distance_matching"`',
        ),
        "edges": PedestrianEdgesCleanFile,
        "trips": SurveyedTripsFile,
    }
    output_files = {"distances": SurveyedTripsPedestrianDistancesFile}

    def run(self):
        import polars as pl

        assert self.exec_path is not None

        edges_gdf = self.input["edges"].read()
        edges = pl.from_pandas(edges_gdf.loc[:, ["edge_id", "source", "target", "length"]]).rename(
            {"length": "weight"}
        )

        if self.uses_distance_matching():
            df = self.trip_level_distances(edges)
        else:
            df = self.medoids_distances(self.input["trips"].read(), edges)
        self.output["distances"].write(df)

    def trip_level_distances(self, edges: pl.DataFrame) -> pl.DataFrame:
        import polars as pl

        assert self.exec_path is not None
        node_pairs = (
            self.input["trip_nodes"]
            .read()
            .select(
                "origin_node",
                "destination_node",
                survey_trip_id="trip_id",
                trip_id=pl.int_range(pl.len(), dtype=pl.UInt64),
            )
        )
        results = trip_routing(node_pairs, edges, self.exec_path, with_routes=False)
        return node_pairs.join(results, on="trip_id").select(
            trip_id="survey_trip_id", pedestrian_distance=pl.col("value").cast(pl.Float64)
        )

    def medoids_distances(self, trips: pl.DataFrame, edges: pl.DataFrame) -> pl.DataFrame:
        import numpy as np
        import polars as pl

        assert self.exec_path is not None
        check_has_cols(trips, ["origin_detailed_zone", "destination_detailed_zone"])
        trips = trips.rename(
            {"origin_detailed_zone": "origin", "destination_detailed_zone": "destination"}
        )
        od_pairs = trips.select("origin", "destination").unique().drop_nulls()

        nodes = self.input["nodes"].read()
        node_pairs = (
            od_pairs.join(nodes, left_on="origin", right_on="zone_id", how="inner")
            .join(nodes, left_on="destination", right_on="zone_id", how="inner")
            .select(
                trip_id=pl.int_range(pl.len(), dtype=pl.UInt64),
                origin="origin",
                destination="destination",
                origin_node="node",
                destination_node="node_right",
                weight=pl.col("weight") * pl.col("weight_right"),
            )
        )
        pairs_per_zone = int((len(nodes) / nodes["zone_id"].n_unique()) ** 2)

        results = trip_routing(node_pairs, edges, self.exec_path, with_routes=False)
        node_pairs = node_pairs.join(results, on="trip_id")
        # Compute a single distance for each OD pair from the weighted median.
        distances = node_pairs["value"].to_numpy().reshape(-1, pairs_per_zone)
        weights = node_pairs["weight"].to_numpy().reshape(-1, pairs_per_zone)
        medians = np.quantile(distances, 0.5, axis=1, weights=weights, method="inverted_cdf")

        od_pairs = (
            node_pairs.select("origin", "destination")
            .unique(maintain_order=True)
            .with_columns(pedestrian_distance=pl.Series(medians))
        )
        return (
            trips.select("trip_id", "origin", "destination")
            .join(od_pairs, on=["origin", "destination"], how="inner")
            .drop("origin", "destination")
        )


class SurveyedTripsOpenTripPlannerStep(StepWithSurveyODMethod, OpenTripPlannerStep):
    """Computes the surveyed trips' travel time and generalized time by public transit with
    OpenTripPlanner.

    The origin / destination coordinates depend on the
    [`od_method`](parameters.md#surveyod_method) parameter.
    Check the TripsOpenTripPlannerStep for additional details.
    """

    input_files = {
        "medoids": InputFile(
            SurveyedZonesMedoidsFile,
            when=lambda inst: inst.uses_medoids(),
            when_doc='if `od_method` is `"medoids"`',
        ),
        "origins": InputFile(
            SurveyedTripsOriginsFile,
            when=lambda inst: inst.uses_distance_matching(),
            when_doc='if `od_method` is `"distance_matching"`',
        ),
        "destinations": InputFile(
            SurveyedTripsDestinationsFile,
            when=lambda inst: inst.uses_distance_matching(),
            when_doc='if `od_method` is `"distance_matching"`',
        ),
        "trips": SurveyedTripsFile,
    }
    output_files = {"costs": SurveyedTripsPublicTransitItinerariesFile}

    def run(self):
        import polars as pl

        trips = self.input["trips"].read()
        check_has_cols(trips, ["departure_time", "arrival_time"])

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
        if self.uses_distance_matching():
            trips = trips.select("trip_id", "time", "arrive_by").drop_nulls()
            df = self.trip_level_queries(trips)
        else:
            check_has_cols(trips, ["origin_detailed_zone", "destination_detailed_zone"])
            trips = trips.select(
                "trip_id",
                "time",
                "arrive_by",
                origin="origin_detailed_zone",
                destination="destination_detailed_zone",
            ).drop_nulls()
            df = self.medoids_queries(trips)
        # Note. The waiting_time and legs are not saved due to the difficulty of aggregating
        # multiple trips.
        df = df.select("trip_id", "travel_time", "generalized_time").sort("trip_id")
        self.output["costs"].write(df)

    def trip_level_queries(self, trips: pl.DataFrame) -> pl.DataFrame:
        import polars as pl

        def lng_lat(name: str, prefix: str) -> pl.DataFrame:
            gdf = self.input[name].read().to_crs("EPSG:4326")
            return pl.DataFrame(
                {
                    "trip_id": gdf["trip_id"].to_numpy(),
                    f"{prefix}_lng": gdf.geometry.x.to_numpy(),
                    f"{prefix}_lat": gdf.geometry.y.to_numpy(),
                }
            )

        trips = (
            trips.join(lng_lat("origins", "origin"), on="trip_id", how="inner")
            .join(lng_lat("destinations", "destination"), on="trip_id", how="inner")
            .sort("trip_id")
        )
        return self.run_queries(trips)

    def medoids_queries(self, trips: pl.DataFrame) -> pl.DataFrame:
        import numpy as np
        import polars as pl

        trips = trips.sort("trip_id")
        medoids = self.input["medoids"].read().to_crs("EPSG:4326")
        medoids["lng"] = medoids["geometry"].x
        medoids["lat"] = medoids["geometry"].y
        medoids_df = pl.from_pandas(medoids[["zone_id", "weight", "lng", "lat"]])

        trips = (
            trips.join(
                medoids_df,
                left_on="origin",
                right_on="zone_id",
                how="inner",
                maintain_order="left_right",
            )
            .join(
                medoids_df,
                left_on="destination",
                right_on="zone_id",
                how="inner",
                maintain_order="left_right",
            )
            .select(
                id="trip_id",
                index=pl.int_range(pl.len()).over("trip_id"),
                origin_lng="lng",
                origin_lat="lat",
                destination_lng="lng_right",
                destination_lat="lat_right",
                weight=pl.col("weight") * pl.col("weight_right"),
                time="time",
                arrive_by="arrive_by",
            )
            .with_columns(
                trip_id=pl.col("id").cast(pl.String) + "-" + pl.col("index").cast(pl.String)
            )
        )

        df = self.run_queries(trips)
        df = df.join(trips.select("trip_id", "weight"), on="trip_id")
        df = df.sort("trip_id")

        trips = trips.sort("trip_id").select(trip_id=pl.col("id").unique(maintain_order=True))
        values_per_trip = int(len(df) / len(trips))
        # Compute a single value for each OD pair / departure time from the weighted median.
        for col in ("travel_time", "generalized_time"):
            # Set weight to zero for NULL values.
            df = df.with_columns(
                w=pl.when(pl.col(col).is_null().all().over("trip_id"))
                .then(1.0)
                .when(pl.col(col).is_null())
                .then(0.0)
                .otherwise("weight")
            )
            values = df[col].to_numpy().reshape(-1, values_per_trip)
            weights = df["w"].to_numpy().reshape(-1, values_per_trip)
            medians = np.quantile(values, 0.5, axis=1, weights=weights, method="inverted_cdf")
            trips = trips.with_columns(pl.Series(medians).alias(col))
        return trips


class SurveyedRoadODNodesFromMedoidsStep(StepWithRoadForbiddenTypes):
    """Identifies nodes on the road network to be used as origin / destination for each
    surveyed zone from their medoids.
    """

    input_files = {"edges": RoadEdgesCleanFile, "medoids": SurveyedZonesMedoidsFile}
    output_files = {"nodes": SurveyedZonesRoadNodesFile}
    priority = 0

    def run(self):
        import polars as pl

        medoids = self.input["medoids"].read()
        medoids["medoid_id"] = medoids["zone_id"].astype(str) + "-" + medoids["index"].astype(str)

        edges = self.input["edges"].read()
        edges = edges.loc[
            ~edges["edge_type"].isin(self.forbidden_types),
            ["edge_id", "geometry", "source", "target"],
        ]

        edges = create_source_target_points(edges)
        nodes = identify_nodes(edges, medoids, "medoid_id")

        nodes = nodes.join(pl.from_pandas(medoids.drop(columns="geometry")), on="medoid_id").select(
            "zone_id", "index", "weight", "node"
        )
        self.output["nodes"].write(nodes)


def draw_distance_matched_od_points(
    candidates: pl.DataFrame,
    trips: pl.DataFrame,
    nb_draws: int,
    rng: np.random.Generator,
    chunk_size: int = 10_000,
) -> pl.DataFrame:
    """Draws an origin and destination point for each trip so that their Euclidean distance matches
    (approximately) the trip's target distance.

    candidates: DataFrame of candidate points, with columns `location_id`, `x`, `y` (a location
        is a group of candidate points, e.g., the nodes in a zone; a location with a single point
        always yields that point).
    trips: DataFrame with columns `trip_id`, `origin`, `destination` (location ids) and `target`
        (target Euclidean distance, in meters; NULL values yield a uniformly random pair).
    nb_draws: Number of random pairs of points drawn for each trip (the closest one is selected).

    Returns a DataFrame with columns `trip_id`, `origin_x`, `origin_y`, `destination_x`,
    `destination_y` and `od_distance`.
    Trips whose origin or destination location has no candidate point are discarded.
    """
    import numpy as np
    import polars as pl

    candidates = candidates.sort("location_id", "x", "y")
    index = (
        candidates.with_row_index("offset")
        .group_by("location_id", maintain_order=True)
        .agg(pl.col("offset").first(), count=pl.len())
    )
    trips = trips.join(
        index.rename({"offset": "o_offset", "count": "o_count"}),
        left_on="origin",
        right_on="location_id",
        how="inner",
    ).join(
        index.rename({"offset": "d_offset", "count": "d_count"}),
        left_on="destination",
        right_on="location_id",
        how="inner",
    )

    xy = candidates.select("x", "y").to_numpy()

    selected_o = list()
    selected_d = list()
    for chunk in trips.iter_slices(chunk_size):
        n = len(chunk)
        o_idx = chunk["o_offset"].to_numpy()[:, None] + np.floor(
            rng.random((n, nb_draws)) * chunk["o_count"].to_numpy()[:, None]
        ).astype(np.int64)
        d_idx = chunk["d_offset"].to_numpy()[:, None] + np.floor(
            rng.random((n, nb_draws)) * chunk["d_count"].to_numpy()[:, None]
        ).astype(np.int64)
        dists = np.hypot(xy[o_idx, 0] - xy[d_idx, 0], xy[o_idx, 1] - xy[d_idx, 1])
        target = chunk["target"].cast(pl.Float64).fill_null(np.nan).to_numpy()
        # For NULL targets, the error is NaN for all draws: select the first (random) draw.
        errors = np.nan_to_num(np.abs(dists - target[:, None]), nan=0.0)
        best = np.argmin(errors, axis=1)
        rows = np.arange(n)
        selected_o.append(o_idx[rows, best])
        selected_d.append(d_idx[rows, best])

    o_sel = np.concatenate(selected_o) if selected_o else np.array([], dtype=np.int64)
    d_sel = np.concatenate(selected_d) if selected_d else np.array([], dtype=np.int64)
    return pl.DataFrame(
        {
            "trip_id": trips["trip_id"],
            "origin_x": xy[o_sel, 0],
            "origin_y": xy[o_sel, 1],
            "destination_x": xy[d_sel, 0],
            "destination_y": xy[d_sel, 1],
            "od_distance": np.hypot(xy[o_sel, 0] - xy[d_sel, 0], xy[o_sel, 1] - xy[d_sel, 1]),
        }
    )


class SurveyedTripsODPointsStep(StepWithPedestrianForbiddenTypes, RandomStep):
    """Draws an origin and destination point for each surveyed trip, by matching the trip's
    reported Euclidean distance.

    The candidate points are the nodes of the pedestrian network located in the trip's origin /
    destination zone (nodes of edges whose type is specified in the
    [`forbidden_types`](parameters.md#pedestrian_networkforbidden_types) parameter are excluded).
    For each trip, random pairs of candidate points are drawn and the pair whose Euclidean distance
    is the closest to the trip's `trip_euclidean_distance_km` is selected.
    For trips with no reported Euclidean distance, a random pair is selected.

    When the trip's origin (resp. destination) is a special location of the survey (column
    `origin_special_location`, resp. `destination_special_location`), the coordinates of that
    special location are used as origin (resp. destination) point and only the other end is drawn
    (when both ends are special locations, the pair is fixed and can still be discarded by
    `max_od_distance_error`).

    The same points are used for all modes (they are then matched to the nearest node of each
    network).
    If [`max_od_distance_error`](parameters.md#surveymax_od_distance_error) is set, the trips for
    which the selected pair does not match the reported distance closely enough (or with no
    reported distance) are discarded, i.e., they have no travel time / distance for any mode.
    """

    nb_draws = IntParameter(
        "survey.nb_od_pair_draws",
        default=100,
        lower_bound=1,
        description=(
            "Number of random pairs of origin / destination points drawn for each surveyed trip, "
            "when matching the trips' Euclidean distance."
        ),
        note="Larger values lead to a closer match but increase the running time and memory usage",
    )
    max_od_distance_error = FloatParameter(
        "survey.max_od_distance_error",
        lower_bound=0.0,
        description=(
            "Maximum absolute difference, in meters, between the trips' reported Euclidean "
            "distance and the Euclidean distance between their selected origin and destination "
            "points."
        ),
        note=(
            "Trips exceeding the threshold, or with no reported Euclidean distance, are discarded "
            "for all modes. If not set, all trips are kept"
        ),
    )
    input_files = {
        "edges": PedestrianEdgesCleanFile,
        "trips": SurveyedTripsFile,
        "zones": SurveyedDetailedZonesFile,
        "special_locations": InputFile(SurveyedSpecialLocationsFile, optional=True),
    }
    output_files = {
        "origins": SurveyedTripsOriginsFile,
        "destinations": SurveyedTripsDestinationsFile,
    }
    priority = 0

    def run(self):
        import geopandas as gpd
        import polars as pl

        assert self.nb_draws is not None
        assert self.forbidden_types is not None

        edges = self.input["edges"].read()
        edges = edges.loc[
            ~edges["edge_type"].isin(self.forbidden_types), ["source", "target", "geometry"]
        ]
        sources = edges.drop_duplicates(subset=["source"])
        targets = edges.drop_duplicates(subset=["target"])
        nodes = pl.concat(
            [
                pl.DataFrame(
                    {
                        "node": sources["source"].to_numpy(),
                        "x": sources["geometry"].apply(lambda g: g.coords[0][0]).to_numpy(),
                        "y": sources["geometry"].apply(lambda g: g.coords[0][1]).to_numpy(),
                    }
                ),
                pl.DataFrame(
                    {
                        "node": targets["target"].to_numpy(),
                        "x": targets["geometry"].apply(lambda g: g.coords[-1][0]).to_numpy(),
                        "y": targets["geometry"].apply(lambda g: g.coords[-1][1]).to_numpy(),
                    }
                ),
            ]
        ).unique("node")
        nodes_gdf = gpd.GeoDataFrame(
            geometry=gpd.points_from_xy(nodes["x"], nodes["y"]), crs=edges.crs
        )
        zones = self.input["zones"].read().to_crs(edges.crs)
        zones = zones[["detailed_zone_id", "geometry"]].rename(
            columns={"detailed_zone_id": "zone_id"}
        )
        nodes_gdf = nodes_gdf.sjoin(zones, predicate="intersects", how="inner")

        def to_candidates(gdf: gpd.GeoDataFrame, id_col: str, prefix: str) -> pl.DataFrame:
            # Location ids are prefixed so that zone ids and special location ids cannot collide.
            return pl.DataFrame(
                {
                    "location_id": prefix + gdf[id_col].astype(str).to_numpy(),
                    "x": gdf.geometry.x.to_numpy(),
                    "y": gdf.geometry.y.to_numpy(),
                }
            )

        # Each special location is a location with a single candidate point (its coordinates).
        special_gdf = self.input["special_locations"].read_if_exists()
        if special_gdf is None:
            special_ids = []
            candidates = to_candidates(nodes_gdf, "zone_id", "zone:")
        else:
            special_gdf = special_gdf.to_crs(edges.crs)
            special_ids = special_gdf["special_location_id"].astype(str).tolist()
            candidates = pl.concat(
                [
                    to_candidates(nodes_gdf, "zone_id", "zone:"),
                    to_candidates(special_gdf, "special_location_id", "special:"),
                ]
            )

        trips = self.input["trips"].read()
        check_has_cols(
            trips,
            ["origin_detailed_zone", "destination_detailed_zone", "trip_euclidean_distance_km"],
        )

        def location_key(end: str) -> pl.Expr:
            """The trip's special location (if known), otherwise its detailed zone."""
            zone_key = pl.lit("zone:") + pl.col(f"{end}_detailed_zone").cast(pl.String)
            if f"{end}_special_location" not in trips.columns:
                return zone_key
            special = pl.col(f"{end}_special_location").cast(pl.String)
            return (
                pl.when(special.is_in(special_ids))
                .then(pl.lit("special:") + special)
                .otherwise(zone_key)
            )

        trips = trips.select(
            "trip_id",
            origin=location_key("origin"),
            destination=location_key("destination"),
            target=pl.col("trip_euclidean_distance_km") * 1000,
        ).drop_nulls(["origin", "destination"])
        nb_special = {
            end: trips[end].str.starts_with("special:").sum() for end in ("origin", "destination")
        }
        logger.info(
            f"Using special locations for {nb_special['origin']} origins and "
            f"{nb_special['destination']} destinations"
        )

        points = draw_distance_matched_od_points(
            candidates, trips, self.nb_draws, self.get_rng(str(self))
        )
        n = len(trips) - len(points)
        if n:
            logger.warning(
                f"Discarding {n} trips with no pedestrian node in origin / destination zone (or "
                "unknown zone)"
            )
        points = points.join(trips.select("trip_id", "target"), on="trip_id")
        error = (pl.col("od_distance") - pl.col("target")).abs()
        stats = points.select(median=error.median(), q90=error.quantile(0.9), max=error.max()).row(
            0, named=True
        )
        logger.info(
            "Absolute difference between reported and selected OD distance: "
            f"median = {stats['median']:.0f} m, 90th percentile = {stats['q90']:.0f} m, "
            f"max = {stats['max']:.0f} m"
            if stats["median"] is not None
            else "No trip with a reported Euclidean distance"
        )
        if self.max_od_distance_error is not None:
            n = len(points)
            points = points.filter(error <= self.max_od_distance_error)
            n -= len(points)
            if n:
                logger.warning(
                    f"Discarding {n} trips whose OD distance does not match the reported Euclidean "
                    f"distance (or with no reported distance)"
                )

        for name, prefix in (("origins", "origin"), ("destinations", "destination")):
            gdf = gpd.GeoDataFrame(
                {"trip_id": points["trip_id"].to_numpy()},
                geometry=gpd.points_from_xy(points[f"{prefix}_x"], points[f"{prefix}_y"]),
                crs=edges.crs,
            )
            self.output[name].write(gdf)


class SurveyedPedestrianODNodesFromPointsStep(StepWithPedestrianForbiddenTypes):
    """Identifies nodes on the pedestrian network to be used as origin / destination for each
    surveyed trip, from the trips' origin / destination points.

    The origin / destination node is the source or target (whichever is closer) of the nearest
    pedestrian edge to the point.
    """

    input_files = {
        "edges": PedestrianEdgesCleanFile,
        "origins": SurveyedTripsOriginsFile,
        "destinations": SurveyedTripsDestinationsFile,
    }
    output_files = {"nodes": SurveyedTripsPedestrianNodesFile}
    priority = 0

    def run(self):
        assert self.forbidden_types is not None
        nodes = snap_survey_points(
            self.input["edges"].read(),
            self.forbidden_types,
            self.input["origins"].read(),
            self.input["destinations"].read(),
        )
        self.output["nodes"].write(nodes)


class SurveyedRoadODNodesFromPointsStep(StepWithRoadForbiddenTypes):
    """Identifies nodes on the road network to be used as origin / destination for each surveyed
    trip, from the trips' origin / destination points.

    The origin / destination node is the source or target (whichever is closer) of the nearest
    road edge to the point.
    Edges whose type is specified in the
    [`forbidden_types`](parameters.md#road_networkforbidden_types) parameter are excluded.
    """

    input_files = {
        "edges": RoadEdgesCleanFile,
        "origins": SurveyedTripsOriginsFile,
        "destinations": SurveyedTripsDestinationsFile,
    }
    output_files = {"nodes": SurveyedTripsRoadNodesFile}
    priority = 0

    def run(self):
        assert self.forbidden_types is not None
        nodes = snap_survey_points(
            self.input["edges"].read(),
            self.forbidden_types,
            self.input["origins"].read(),
            self.input["destinations"].read(),
        )
        self.output["nodes"].write(nodes)


class SurveyedTripsCarTravelTimesStep(StepWithSurveyODMethod, RoutingCLIStep):
    """Computes the surveyed trips' travel time on the road network by car, under congested
    conditions.

    Congested conditions are read from the ex-ante simulation.

    The origin / destination road nodes are selected according to the
    [`od_method`](parameters.md#surveyod_method) parameter:

    - `"medoids"`: the shortest paths are computed between all pairs of medoids of the origin and
      destination zones, and the travel time / distance of a trip is the weighted median over these
      pairs (all trips with the same origin zone, destination zone and departure time have the
      same travel time).
    - `"distance_matching"`: the road nodes are the nearest nodes to the trip's origin /
      destination points, whose Euclidean distance matches the trip's reported Euclidean distance
      (see `SurveyedTripsODPointsStep`).
    """

    input_files = {
        "nodes": InputFile(
            SurveyedZonesRoadNodesFile,
            when=lambda inst: inst.uses_medoids(),
            when_doc='if `od_method` is `"medoids"`',
        ),
        "trip_nodes": InputFile(
            SurveyedTripsRoadNodesFile,
            when=lambda inst: inst.uses_distance_matching(),
            when_doc='if `od_method` is `"distance_matching"`',
        ),
        "trips": SurveyedTripsFile,
        "edges": RoadEdgesCleanFile,
        "edges_fftt": RoadEdgesFreeFlowTravelTimeFile,
        "congestion_conditions": MetroExAnteSimulatedTravelTimeFunctionsFile,
    }
    output_files = {"tt": SurveyedTripsCarTravelTimesFile}

    def run(self):
        import polars as pl

        assert self.exec_path is not None

        edges_gdf = self.input["edges"].read()
        edges_ttfs = (
            self.input["congestion_conditions"]
            .read()
            .filter(vehicle_id=repr(CarDriverAloneVehicle))
        )
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
        trips = trips.with_columns(departure_time=pl.duration(minutes="departure_time"))

        if self.uses_distance_matching():
            df = self.trip_level_travel_times(trips, edges, edges_ttfs)
        else:
            df = self.medoids_travel_times(trips, edges, edges_ttfs)
        self.output["tt"].write(df)

    def route(
        self, node_pairs: pl.DataFrame, edges: pl.DataFrame, edges_ttfs: pl.DataFrame
    ) -> pl.DataFrame:
        """Runs the routing for the node pairs and returns them with `travel_time` (in seconds) and
        `distance` columns.
        """
        import polars as pl

        assert self.exec_path is not None
        results = trip_routing(
            node_pairs, edges, self.exec_path, with_routes=True, network_conditions=edges_ttfs
        )
        return node_pairs.join(results, on="trip_id").with_columns(
            travel_time=pl.col("value")
            - pl.col("departure_time").dt.total_seconds(fractional=True),
            distance=pl.col("route")
            .list.eval(pl.element().replace_strict(edges["edge_id"], edges["length"]))
            .list.sum(),
        )

    def trip_level_travel_times(
        self, trips: pl.DataFrame, edges: pl.DataFrame, edges_ttfs: pl.DataFrame
    ) -> pl.DataFrame:
        import polars as pl

        trip_nodes = self.input["trip_nodes"].read()
        node_pairs = (
            trips.select("trip_id", "departure_time")
            .drop_nulls()
            .join(trip_nodes, on="trip_id", how="inner")
            .select(
                survey_trip_id="trip_id",
                trip_id=pl.int_range(pl.len(), dtype=pl.UInt64),
                origin_node="origin_node",
                destination_node="destination_node",
                departure_time="departure_time",
            )
        )
        node_pairs = self.route(node_pairs, edges, edges_ttfs)
        return node_pairs.select(
            trip_id="survey_trip_id",
            travel_time=pl.duration(seconds="travel_time"),
            distance=pl.col("distance").cast(pl.Float64),
        )

    def medoids_travel_times(
        self, trips: pl.DataFrame, edges: pl.DataFrame, edges_ttfs: pl.DataFrame
    ) -> pl.DataFrame:
        import numpy as np
        import polars as pl

        check_has_cols(trips, ["origin_detailed_zone", "destination_detailed_zone"])
        trips = trips.rename(
            {"origin_detailed_zone": "origin", "destination_detailed_zone": "destination"}
        )
        od_pairs = trips.select("origin", "destination", "departure_time").unique().drop_nulls()

        nodes = self.input["nodes"].read()
        node_pairs = (
            od_pairs.join(nodes, left_on="origin", right_on="zone_id", how="inner")
            .join(nodes, left_on="destination", right_on="zone_id", how="inner")
            .select(
                trip_id=pl.int_range(pl.len(), dtype=pl.UInt64),
                origin="origin",
                destination="destination",
                origin_node="node",
                destination_node="node_right",
                departure_time="departure_time",
                weight=pl.col("weight") * pl.col("weight_right"),
            )
        )
        pairs_per_zone = int((len(nodes) / nodes["zone_id"].n_unique()) ** 2)

        node_pairs = self.route(node_pairs, edges, edges_ttfs)
        # Compute a single travel time and distance for each OD pair / departure time from the
        # weighted median.
        weights = node_pairs["weight"].to_numpy().reshape(-1, pairs_per_zone)
        medians = {
            col: np.quantile(
                node_pairs[col].to_numpy().reshape(-1, pairs_per_zone),
                0.5,
                axis=1,
                weights=weights,
                method="inverted_cdf",
            )
            for col in ("travel_time", "distance")
        }

        od_pairs = (
            node_pairs.select("origin", "destination", "departure_time")
            .unique(maintain_order=True)
            .with_columns(
                seconds=pl.Series(medians["travel_time"]),
                distance=pl.Series(medians["distance"], dtype=pl.Float64),
            )
            .with_columns(travel_time=pl.duration(seconds="seconds"))
            .drop("seconds")
        )
        return (
            trips.select("trip_id", "origin", "destination", "departure_time")
            .join(od_pairs, on=["origin", "destination", "departure_time"], how="inner")
            .drop("origin", "destination", "departure_time")
        )


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
            tours = (
                tours.with_columns(
                    dists=pl.col("trip_ids").list.eval(
                        pl.element().replace_strict(
                            dists["trip_id"], dists["pedestrian_distance"], default=None
                        )
                    )
                )
                .with_columns(
                    pedestrian_distance=pl.when(pl.col("dists").list.contains(None).not_()).then(
                        pl.col("dists").list.sum()
                    )
                )
                .drop("dists")
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
            tours = (
                tours.with_columns(
                    tts=pl.col("trip_ids").list.eval(
                        pl.element().replace_strict(
                            pt_tt["trip_id"], pt_tt["generalized_time"], default=None
                        )
                    )
                )
                .with_columns(
                    travel_time_public_transit=pl.when(
                        pl.col("tts").list.contains(None).not_()
                    ).then(pl.col("tts").list.sum())
                )
                .drop("tts")
            )
        if self.has_car_mode():
            car_tt = self.input["car_tt"].read()
            tours = (
                tours.with_columns(
                    tts=pl.col("trip_ids").list.eval(
                        pl.element().replace_strict(
                            car_tt["trip_id"], car_tt["travel_time"], default=None
                        )
                    )
                )
                .with_columns(
                    travel_time_car=pl.when(pl.col("tts").list.contains(None).not_()).then(
                        pl.col("tts").list.sum()
                    )
                )
                .drop("tts")
            )
            if "distance" in car_tt.columns:
                tours = (
                    tours.with_columns(
                        dists=pl.col("trip_ids").list.eval(
                            pl.element().replace_strict(
                                car_tt["trip_id"], car_tt["distance"], default=None
                            )
                        )
                    )
                    .with_columns(
                        distance_car=pl.when(pl.col("dists").list.contains(None).not_()).then(
                            pl.col("dists").list.sum()
                        )
                    )
                    .drop("dists")
                )
        tours = tours.drop("trip_ids")
        self.output["tt"].write(tours)


class SurveyedTripsTravelTimeComparisonStep(PlotStep, StepWithWalkingSpeed, StepWithBicycleSpeed):
    """Compares reported and simulated travel times for the survey trips, for each mode."""

    input_files = {
        "trips": SurveyedTripsFile,
        "pedestrian_dist": SurveyedTripsPedestrianDistancesFile,
        "pt_tt": SurveyedTripsPublicTransitItinerariesFile,
        "car_tt": SurveyedTripsCarTravelTimesFile,
    }
    output_files = {
        "car": SurveyedTripsTravelTimeComparisonCarPlotFile,
        "walking": SurveyedTripsTravelTimeComparisonWalkingPlotFile,
        "bicycle": SurveyedTripsTravelTimeComparisonBicyclePlotFile,
        "public_transit": SurveyedTripsTravelTimeComparisonPublicTransitPlotFile,
    }

    def run(self):
        import polars as pl

        modes = ["car", "public_transit", "walking", "bicycle"]

        trips = self.input["trips"].read()
        trips = (
            trips.select(
                "trip_id",
                observed_tt=pl.duration(minutes=pl.col("travel_time")),
                mode=pl.when(pl.col("main_mode_group").cast(pl.String).str.starts_with("car_"))
                .then(pl.lit("car"))
                .otherwise(pl.col("main_mode_group").cast(pl.String)),
            )
            .filter(pl.col("mode").is_in(modes))
            .drop_nulls()
        )

        simulated_tts = dict()
        dists = self.input["pedestrian_dist"].read()
        simulated_tts["walking"] = dists.select(
            "trip_id",
            simulated_tt=pl.duration(
                hours=pl.col("pedestrian_distance") / 1000 / self.walking_speed
            ),
        )
        simulated_tts["bicycle"] = dists.select(
            "trip_id",
            simulated_tt=pl.duration(
                hours=pl.col("pedestrian_distance") / 1000 / self.bicycle_speed
            ),
        )

        pt_tts = self.input["pt_tt"].read()
        simulated_tts["public_transit"] = pt_tts.select("trip_id", simulated_tt="generalized_time")

        car_tts = self.input["car_tt"].read()
        simulated_tts["car"] = car_tts.select("trip_id", simulated_tt="travel_time")

        for mode, sim_df in simulated_tts.items():
            label = mode.replace("_", " ")
            df = trips.filter(mode=mode).join(sim_df, on="trip_id", how="inner").drop_nulls()
            # df = df.filter(pl.col("simulated_tt") > 0, pl.col("observed_tt") > 0)
            if df.height < 2:
                continue
            observed = df["observed_tt"].dt.total_seconds().to_numpy()
            predicted = df["simulated_tt"].dt.total_seconds().to_numpy()
            rmse = float(((observed - predicted) ** 2).mean() ** 0.5)
            fig = plot_travel_time_comparison(
                predicted,
                observed,
                rmse,
                xlabel=f"Simulated travel time ({label})",
                ylabel=f"Observed travel time ({label})",
                show_regression=True,
                outlier_quantile=0.95,
            )
            self.output[mode].write(fig)
