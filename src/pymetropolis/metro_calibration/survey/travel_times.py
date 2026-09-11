from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.plots import plot_travel_time_comparison
from pymetropolis.metro_demand.modes.bicycle import StepWithBicycleSpeed
from pymetropolis.metro_demand.modes.walking import StepWithWalkingSpeed
from pymetropolis.metro_demand.routing.od_pairs import (
    StepWithPedestrianForbiddenTypes,
    StepWithRoadForbiddenTypes,
    create_source_target_points,
    identify_nodes,
)
from pymetropolis.metro_demand.routing.opentripplanner import OpenTripPlannerStep
from pymetropolis.metro_demand.routing.routing_cli import RoutingCLIStep, trip_routing
from pymetropolis.metro_network.pedestrian_network.files import PedestrianEdgesCleanFile
from pymetropolis.metro_network.road_network.files import (
    RoadEdgesCleanFile,
    RoadEdgesFreeFlowTravelTimeFile,
)
from pymetropolis.metro_pipeline.parameters import IntParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_simulation.run.files import MetroExAnteSimulatedTravelTimeFunctionsFile
from pymetropolis.modes import StepWithModes
from pymetropolis.random import RandomStep

from .files import (
    SurveyedDetailedZonesFile,
    SurveyedToursFile,
    SurveyedToursTravelTimesFile,
    SurveyedTripsCarTravelTimesFile,
    SurveyedTripsFile,
    SurveyedTripsPedestrianDistancesFile,
    SurveyedTripsPublicTransitItinerariesFile,
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
    import polars as pl


def check_has_cols(df: pl.DataFrame, cols: list[str]):
    missing_cols = set(cols) - set(df.columns)
    if missing_cols:
        missing_cols_str = ", ".join(missing_cols)
        raise MetropyError(f"Missing columns in surveyed trips: {missing_cols_str}")


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
        from itertools import cycle, islice

        import geopandas as gpd
        import numpy as np
        import polars as pl
        from shapely.geometry import Point
        from sklearn_extra.cluster import KMedoids

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
        nodes = gpd.GeoDataFrame(
            edges.groupby("source").agg({"length": "sum", "geometry": "first"}), crs=edges.crs
        )
        nodes = nodes.reset_index(names="node_id")
        nodes["geometry"] = nodes["geometry"].apply(lambda geom: Point(geom.coords[0]))
        nodes = nodes.sjoin(zones.to_crs(nodes.crs), predicate="intersects", how="inner")
        nodes["x"] = nodes.geometry.x
        nodes["y"] = nodes.geometry.y
        nodes_df = pl.from_pandas(nodes.drop(columns=["geometry", "index_right"]))

        medoids = list()
        for (zone_id,), zone_nodes in nodes_df.partition_by(
            "zone_id", include_key=False, as_dict=True
        ).items():
            X = zone_nodes.select("x", "y").to_numpy()
            if len(X) < self.nb_clusters:
                # There are fewer nodes than clusters: return each node as a center.
                # Cycle the nodes so that the number of centers stay fixed.
                centers = np.fromiter(
                    islice(cycle(X), self.nb_clusters), dtype=np.dtype((float, 2))
                )
                weights = np.repeat(1 / len(centers), len(centers))
            else:
                kmedoids = KMedoids(n_clusters=self.nb_clusters, random_state=self.random_seed).fit(
                    X
                )
                centers = kmedoids.cluster_centers_
                values_by_cluster = (
                    zone_nodes.with_columns(cluster=pl.Series(kmedoids.labels_))
                    .group_by("cluster")
                    .agg(pl.col("length").sum())
                    .with_columns(weight=pl.col("length") / pl.col("length").sum())
                    .sort("cluster")
                )
                weights = values_by_cluster["weight"].to_numpy()
            medoids.extend(
                [
                    [zone_id, i + 1, weights[i], centers[i][0], centers[i][1]]
                    for i in range(len(centers))
                ]
            )
        medoids_df = pl.DataFrame(
            medoids, schema=["zone_id", "index", "weight", "x", "y"], orient="row"
        )
        n = medoids_df["zone_id"].n_unique()
        if n < len(zones):
            logger.warning(f"No pedestrian node in zone for {len(zones) - n} zones.")
        medoids_gdf = gpd.GeoDataFrame(
            data=medoids_df.drop("x", "y").to_pandas(),
            geometry=gpd.GeoSeries.from_xy(medoids_df["x"], medoids_df["y"], crs=edges.crs),
        )
        self.output["medoids"].write(medoids_gdf)


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


class SurveyedTripsPedestrianDistancesStep(RoutingCLIStep):
    """Computes the surveyed trips' distance on the pedestrian network.

    The distance is defined as the length of the shortest path from origin to destination node on
    the pedestrian network.
    """

    input_files = {
        "nodes": SurveyedZonesPedestrianNodesFile,
        "edges": PedestrianEdgesCleanFile,
        "trips": SurveyedTripsFile,
    }
    output_files = {"distances": SurveyedTripsPedestrianDistancesFile}

    def run(self):
        import numpy as np
        import polars as pl

        assert self.exec_path is not None

        edges_gdf = self.input["edges"].read()
        edges = pl.from_pandas(edges_gdf.loc[:, ["edge_id", "source", "target", "length"]]).rename(
            {"length": "weight"}
        )

        trips = self.input["trips"].read()
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
        df = (
            trips.select("trip_id", "origin", "destination")
            .join(od_pairs, on=["origin", "destination"], how="inner")
            .drop("origin", "destination")
        )
        self.output["distances"].write(df)


class SurveyedTripsOpenTripPlannerStep(OpenTripPlannerStep):
    """Computes the surveyed trips' travel time and generalized time by public transit with
    OpenTripPlanner.

    Check the TripsOpenTripPlannerStep for additional details.
    """

    input_files = {"medoids": SurveyedZonesMedoidsFile, "trips": SurveyedTripsFile}
    output_files = {"costs": SurveyedTripsPublicTransitItinerariesFile}

    def run(self):
        import numpy as np
        import polars as pl

        trips = self.input["trips"].read()
        check_has_cols(
            trips,
            ["departure_time", "arrival_time", "origin_detailed_zone", "destination_detailed_zone"],
        )
        trips = trips.rename(
            {"origin_detailed_zone": "origin", "destination_detailed_zone": "destination"}
        )

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
        trips = trips.select("trip_id", "origin", "destination", "time", "arrive_by")
        # Drop trips with null values.
        trips = trips.drop_nulls()
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
        # Note. The waiting_time and legs are not saved due to the difficulty of aggregating
        # multiple trips.
        trips = trips.sort("trip_id")
        self.output["costs"].write(trips)


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


class SurveyedTripsCarTravelTimesStep(RoutingCLIStep):
    """Computes the surveyed trips' travel time on the road network by car, under congested
    conditions.

    Congested conditions are read from the ex-ante simulation.
    """

    input_files = {
        "nodes": SurveyedZonesRoadNodesFile,
        "trips": SurveyedTripsFile,
        "edges": RoadEdgesCleanFile,
        "edges_fftt": RoadEdgesFreeFlowTravelTimeFile,
        "congestion_conditions": MetroExAnteSimulatedTravelTimeFunctionsFile,
    }
    output_files = {"tt": SurveyedTripsCarTravelTimesFile}

    def run(self):
        import numpy as np
        import polars as pl

        assert self.exec_path is not None

        edges_gdf = self.input["edges"].read()
        edges_ttfs = (
            self.input["congestion_conditions"].read().filter(vehicle_id="car_driver_alone")
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
        check_has_cols(
            trips, ["origin_detailed_zone", "destination_detailed_zone", "departure_time"]
        )
        trips = trips.rename(
            {"origin_detailed_zone": "origin", "destination_detailed_zone": "destination"}
        ).with_columns(departure_time=pl.duration(minutes="departure_time"))
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

        results = trip_routing(
            node_pairs, edges, self.exec_path, with_routes=False, network_conditions=edges_ttfs
        )
        node_pairs = node_pairs.join(results, on="trip_id")
        # Compute a single travel time for each OD pair / departure time from the weighted median.
        travel_times = node_pairs["value"].to_numpy().reshape(-1, pairs_per_zone)
        weights = node_pairs["weight"].to_numpy().reshape(-1, pairs_per_zone)
        medians = np.quantile(travel_times, 0.5, axis=1, weights=weights, method="inverted_cdf")

        od_pairs = (
            node_pairs.select("origin", "destination", "departure_time")
            .unique(maintain_order=True)
            .with_columns(seconds=pl.Series(medians))
            .with_columns(travel_time=pl.duration(seconds="seconds"))
            .drop("seconds")
        )
        df = (
            trips.select("trip_id", "origin", "destination", "departure_time")
            .join(od_pairs, on=["origin", "destination", "departure_time"], how="inner")
            .drop("origin", "destination", "departure_time")
        )
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
        tours = tours.drop("trip_ids")
        self.output["tt"].write(tours)


class SurveyedTripsTravelTimeComparisonStep(StepWithWalkingSpeed, StepWithBicycleSpeed):
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
