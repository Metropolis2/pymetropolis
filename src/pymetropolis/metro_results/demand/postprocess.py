from __future__ import annotations

from typing import TYPE_CHECKING

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_demand.modes.park_and_ride.files import ParkAndRideStopsFile
from pymetropolis.metro_demand.modes.park_and_ride.transfer_stops import park_and_ride_car_legs
from pymetropolis.metro_demand.population.files import TripsFile
from pymetropolis.metro_demand.routing.files import (
    NonPrimaryCarTrips,
    NonPrimaryParkAndRideCarTrips,
    PrimaryCarTripsAccessEgressFile,
    PrimaryParkAndRideCarTripsAccessEgressFile,
)
from pymetropolis.metro_network.road_network.files import RoadEdgesFreeFlowTravelTimeFile
from pymetropolis.metro_pipeline import PopulationStep
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_simulation.demand.files import MetroTripsPopulationFile
from pymetropolis.metro_simulation.run import MetroAgentResultsFile, MetroTripResultsFile
from pymetropolis.metro_simulation.run.files import MetroRouteResultsFile
from pymetropolis.modes import ParkAndRide

from .files import ActivityResultsFile, RouteResultsFile, TourResultsFile, TripResultsFile

if TYPE_CHECKING:
    import polars as pl

    from pymetropolis.metro_pipeline.file import MetroFile

# Optional input files for the car parts of park-and-ride trips.
PARK_AND_RIDE_INPUT_FILES = {
    "pr_stops": InputFile(ParkAndRideStopsFile, optional=True),
    "pr_access_egress_parts": InputFile(PrimaryParkAndRideCarTripsAccessEgressFile, optional=True),
    "pr_secondary_trips": InputFile(NonPrimaryParkAndRideCarTrips, optional=True),
}


def read_park_and_ride_car_legs(trips: pl.DataFrame, stops_file: MetroFile) -> pl.DataFrame | None:
    """Returns the original trip id (`trip_id`) and the car leg id (`car_leg_id`) of the trips with
    a car part when traveling by park-and-ride, or `None` if park-and-ride is not simulated.
    """
    if not stops_file.exists():
        return None
    tour_ids = stops_file.read_as_df()["tour_id"]  # ty: ignore[unresolved-attribute]
    return park_and_ride_car_legs(trips, tour_ids).select("trip_id", "car_leg_id")


def read_car_parts(
    car_file: MetroFile, pr_file: MetroFile, pr_legs: pl.DataFrame | None
) -> pl.LazyFrame | None:
    """Reads the parts of car trips (access / egress parts or non-primary trips) of the car modes
    and of the car legs of park-and-ride trips.

    For park-and-ride, the trip ids are replaced by the ids of the car legs, as in the
    Metropolis-Core results.
    Returns `None` if none of the files exists.
    """
    import polars as pl

    lfs = list()
    if car_file.exists():
        lfs.append(car_file.scan())
    if pr_legs is not None and pr_file.exists():
        lfs.append(
            pr_file.scan()
            .join(pr_legs.lazy(), on="trip_id")
            .with_columns(trip_id="car_leg_id")
            .drop("car_leg_id")
        )
    if not lfs:
        return None
    return pl.concat(lfs, how="vertical_relaxed")


class TripResultsStep(PopulationStep):
    """Reads the results from the Metropolis-Core simulation and produces a clean file for results
    at the trip level.
    """

    input_files = {
        "metro_input_trips": MetroTripsPopulationFile,
        "metro_trip_results": MetroTripResultsFile,
        "metro_agent_results": MetroAgentResultsFile,
        "access_egress_parts": InputFile(PrimaryCarTripsAccessEgressFile, optional=True),
        "secondary_trips": InputFile(NonPrimaryCarTrips, optional=True),
        "trips": TripsFile,
        **PARK_AND_RIDE_INPUT_FILES,
    }
    output_files = {"trip_results": TripResultsFile}

    def run(self):
        import polars as pl

        prefix = f"{self.population_name}-"
        trip_results: pl.DataFrame = (
            self.input["metro_trip_results"]
            .scan()
            .filter(pl.col("agent_id").str.starts_with(prefix))
            .collect()
        )
        agent_results: pl.DataFrame = (
            self.input["metro_agent_results"]
            .scan()
            .filter(pl.col("agent_id").str.starts_with(prefix))
            .collect()
        )
        trips = self.input["trips"].read()
        # The car legs of park-and-ride trips are road trips.
        pr_legs = read_park_and_ride_car_legs(trips, self.input["pr_stops"])
        pr_car_leg_ids = (
            pr_legs["car_leg_id"].implode()
            if pr_legs is not None
            else pl.Series([], dtype=pl.String).implode()
        )
        df = trip_results.join(
            agent_results.select("agent_id", "selected_alt_id"), on="agent_id", how="left"
        ).with_columns(pl.col("agent_id").str.strip_prefix(prefix))
        df = df.select(
            trip_id=pl.col("trip_id").str.strip_prefix(prefix),
            tour_id=pl.col("agent_id").str.strip_prefix(prefix),
            mode="selected_alt_id",
            # TODO. Replace this with something more robust when the Mode class is created.
            is_road=pl.col("selected_alt_id").str.starts_with("car_")
            | pl.col("trip_id").str.strip_prefix(prefix).is_in(pr_car_leg_ids),
            departure_time=pl.duration(seconds="departure_time"),
            arrival_time=pl.duration(seconds="arrival_time"),
            route_free_flow_travel_time=pl.duration(seconds="route_free_flow_travel_time"),
            global_free_flow_travel_time=pl.duration(seconds="global_free_flow_travel_time"),
            utility=pl.col("travel_utility") + pl.col("schedule_utility"),
            travel_utility="travel_utility",
            schedule_utility="schedule_utility",
            route_length="length",
            nb_edges="nb_edges",
        )
        access_egress = read_car_parts(
            self.input["access_egress_parts"], self.input["pr_access_egress_parts"], pr_legs
        )
        if access_egress is not None:
            access_egress_columns = access_egress.collect_schema().names()
            # Restrict access / egress to primary road trips.
            access_egress = access_egress.join(
                df.filter(pl.col("nb_edges").is_not_null()).lazy(), on="trip_id", how="semi"
            )
            # Add access / egress values.
            # Note that the constant is already included in the travel utility so it should already
            # account for the access / egress part.
            df: pl.DataFrame = (
                df.lazy()
                .join(access_egress, on="trip_id", how="left")
                .with_columns(
                    pl.col("access_time").fill_null(pl.duration(seconds=0)),
                    pl.col("egress_time").fill_null(pl.duration(seconds=0)),
                    pl.col("access_length").fill_null(0.0),
                    pl.col("egress_length").fill_null(0.0),
                    pl.col("access_path").fill_null([]),
                    pl.col("egress_path").fill_null([]),
                )
                .with_columns(
                    departure_time=pl.col("departure_time") - pl.col("access_time"),
                    arrival_time=pl.col("arrival_time") + pl.col("egress_time"),
                    route_free_flow_travel_time=pl.col("route_free_flow_travel_time")
                    + pl.col("access_time")
                    + pl.col("egress_time"),
                    global_free_flow_travel_time=pl.col("global_free_flow_travel_time")
                    + pl.col("access_time")
                    + pl.col("egress_time"),
                    route_length=pl.col("route_length")
                    + pl.col("access_length")
                    + pl.col("egress_length"),
                    nb_edges=pl.col("nb_edges")
                    + pl.col("access_path").list.len()
                    + pl.col("egress_path").list.len(),
                )
                .drop(set(access_egress_columns) - {"trip_id"})
                .collect()
            )
        secondary_trips = read_car_parts(
            self.input["secondary_trips"], self.input["pr_secondary_trips"], pr_legs
        )
        if secondary_trips is not None:
            secondary_trips = secondary_trips.join(
                df.filter("is_road", pl.col("nb_edges").is_null()).lazy(), on="trip_id", how="semi"
            )
            df: pl.DataFrame = (
                df.lazy()
                .join(secondary_trips, on="trip_id", how="left")
                .with_columns(
                    route_free_flow_travel_time=pl.col("route_free_flow_travel_time").fill_null(
                        pl.col("free_flow_travel_time")
                    ),
                    global_free_flow_travel_time=pl.col("global_free_flow_travel_time").fill_null(
                        pl.col("free_flow_travel_time")
                    ),
                    route_length=pl.col("route_length").fill_null(pl.col("path_length")),
                    nb_edges=pl.col("nb_edges").fill_null(pl.col("path").list.len()),
                )
                .drop("free_flow_travel_time", "path", "path_length")
                .collect()
            )
        # Add vehicle_id.
        input_trips = self.input["metro_input_trips"].read()
        df = df.join(
            input_trips.select("trip_id", mode="alt_id", vehicle_id="class.vehicle"),
            on=["trip_id", "mode"],
            how="left",
        )
        df = merge_park_and_ride_legs(df, trips)
        df = df.with_columns(travel_time=pl.col("arrival_time") - pl.col("departure_time"))
        self.output["trip_results"].write(df)


def merge_park_and_ride_legs(df: pl.DataFrame, trips: pl.DataFrame) -> pl.DataFrame:
    """Merges the legs of park-and-ride trips (car leg + public-transit leg, with ids
    `{trip_id}-1` and `{trip_id}-2`) into a single trip with the original trip id.

    The departure time is the one of the first leg and the arrival time is the one of the last leg.
    Utilities, free-flow travel times, lengths and edge counts are summed over the legs.
    """
    import polars as pl

    is_leg = (pl.col("mode") == repr(ParkAndRide)) & ~pl.col("trip_id").is_in(
        trips["trip_id"].cast(pl.String).implode()
    )
    legs = df.filter(is_leg)
    if legs.is_empty():
        return df
    sum_cols = [
        c
        for c in (
            "route_free_flow_travel_time",
            "global_free_flow_travel_time",
            "utility",
            "travel_utility",
            "schedule_utility",
            "route_length",
            "nb_edges",
        )
        if c in df.columns
    ]
    merged = (
        legs.with_columns(pl.col("trip_id").str.replace(r"-[12]$", ""))
        .sort("trip_id", "departure_time")
        .group_by("trip_id", maintain_order=True)
        .agg(
            *[pl.col(c).first() for c in ("tour_id", "mode") if c in df.columns],
            pl.col("is_road").any(),
            pl.col("departure_time").min(),
            pl.col("arrival_time").max(),
            *[pl.col(c).sum() for c in sum_cols],
            # Vehicle of the car leg.
            pl.col("vehicle_id").drop_nulls().first(),
        )
    )
    return pl.concat((df.filter(~is_leg), merged), how="diagonal_relaxed").select(df.columns)


class RouteResultsStep(PopulationStep):
    """Reads the results from the Metropolis-Core simulation and produces a clean file for route
    results of road trips.
    """

    input_files = {
        "trip_results": TripResultsFile,
        "metro_route_results": MetroRouteResultsFile,
        "access_egress_parts": InputFile(PrimaryCarTripsAccessEgressFile, optional=True),
        "secondary_trips": InputFile(NonPrimaryCarTrips, optional=True),
        "edges_fftt": RoadEdgesFreeFlowTravelTimeFile,
        "trips": TripsFile,
        **PARK_AND_RIDE_INPUT_FILES,
    }
    output_files = {"route_results": RouteResultsFile}

    def run(self):
        import polars as pl

        prefix = f"{self.population_name}-"
        df: pl.DataFrame = (
            self.input["metro_route_results"]
            .scan()
            .select("trip_id", "edge_id", "entry_time", "exit_time")
            .filter(pl.col("trip_id").str.starts_with(prefix))
            .with_columns(pl.col("trip_id").str.strip_prefix(prefix))
            .collect()
        )
        lf = df.lazy()
        pr_legs = read_park_and_ride_car_legs(self.input["trips"].read(), self.input["pr_stops"])
        edges_fftt: pl.LazyFrame = (
            self.input["edges_fftt"].scan().rename({"free_flow_travel_time": "travel_time"})
        )
        # Clean entry / exit time and add travel time.
        lf = lf.with_columns(
            entry_time=pl.duration(seconds="entry_time"), exit_time=pl.duration(seconds="exit_time")
        ).with_columns(travel_time=pl.col("exit_time") - pl.col("entry_time"))
        # Add access / egress part.
        access_egress = read_car_parts(
            self.input["access_egress_parts"], self.input["pr_access_egress_parts"], pr_legs
        )
        if access_egress is not None:
            trip_timings: pl.DataFrame = (
                lf.group_by("trip_id")
                .agg(
                    primary_start=pl.col("entry_time").first(),
                    primary_end=pl.col("exit_time").last(),
                )
                .collect()
            )
            access_edges = (
                access_egress.explode("access_path", empty_as_null=False, keep_nulls=False)
                .select("trip_id", edge_id="access_path")
                .join(edges_fftt, on="edge_id")
                .join(trip_timings.select("trip_id", "primary_start").lazy(), on="trip_id")
                .with_columns(
                    entry_time=pl.col("primary_start")
                    - pl.col("travel_time").cum_sum(reverse=True).over("trip_id")
                )
                .with_columns(exit_time=pl.col("entry_time") + pl.col("travel_time"))
                .select("trip_id", "edge_id", "entry_time", "exit_time", "travel_time")
            )
            egress_edges = (
                access_egress.explode("egress_path", empty_as_null=False, keep_nulls=False)
                .select("trip_id", edge_id="egress_path")
                .join(edges_fftt, on="edge_id")
                .join(trip_timings.select("trip_id", "primary_end").lazy(), on="trip_id")
                .with_columns(
                    exit_time=pl.col("primary_end")
                    + pl.col("travel_time").cum_sum().over("trip_id")
                )
                .with_columns(entry_time=pl.col("exit_time") - pl.col("travel_time"))
                .select("trip_id", "edge_id", "entry_time", "exit_time", "travel_time")
            )
            # Note. `how="vertical_relaxed"` is required when edge ids have been converted to String
            # in the simulation (e.g., in case of HOV edges), while original edge ids are of integer
            # type.
            lf = (
                pl.concat((lf, access_edges, egress_edges), how="vertical_relaxed", rechunk=True)
                .collect()
                .lazy()
            )
        # The routes of the car legs of park-and-ride trips are identified by the original trip id.
        if pr_legs is not None:
            lf = (
                lf.join(pr_legs.lazy(), left_on="trip_id", right_on="car_leg_id", how="left")
                .with_columns(pl.coalesce("trip_id_right", "trip_id").alias("trip_id"))
                .drop("trip_id_right")
            )
        # Read trip results to get road trips not taking the primary network, and their
        # departure time.
        trip_results: pl.LazyFrame = (
            self.input["trip_results"]
            .scan()
            .filter("is_road")
            .select("trip_id", "mode", "departure_time", "arrival_time")
        )
        secondary_trips: pl.DataFrame = trip_results.join(lf, on="trip_id", how="anti").collect()
        if not secondary_trips.is_empty():
            car_secondary: pl.LazyFrame | None = (
                self.input["secondary_trips"].scan()
                if self.input["secondary_trips"].exists()
                else None
            )
            pr_secondary: pl.LazyFrame | None = (
                self.input["pr_secondary_trips"].scan()
                if self.input["pr_secondary_trips"].exists()
                else None
            )
            is_pr = pl.col("mode") == repr(ParkAndRide)
            parts = list()
            if car_secondary is not None:
                parts.append(
                    car_secondary.join(secondary_trips.filter(~is_pr).lazy(), on="trip_id")
                )
            if pr_secondary is not None and pr_legs is not None:
                # The car leg of outbound P+R trips departs at the start of the trip; the car leg of
                # inbound P+R trips arrives at the end of the trip.
                is_outbound = pl.col("car_leg_id").str.ends_with("-1")
                parts.append(
                    pr_secondary.join(secondary_trips.filter(is_pr).lazy(), on="trip_id")
                    .join(pr_legs.lazy(), on="trip_id")
                    .with_columns(
                        departure_time=pl.when(is_outbound)
                        .then("departure_time")
                        .otherwise(pl.col("arrival_time") - pl.col("free_flow_travel_time"))
                    )
                    .drop("car_leg_id")
                )
            if not parts:
                raise MetropyError(
                    "There are some non-primary car trips in the results, "
                    "but the NonPrimaryCarTrips file does not exist."
                )
            # Add secondary trips.
            secondary_routes = (
                pl.concat(parts, how="diagonal_relaxed")
                .explode("path", empty_as_null=False, keep_nulls=False)
                .rename({"path": "edge_id"})
                .join(edges_fftt, on="edge_id")
                .with_columns(
                    exit_time=pl.col("departure_time")
                    + pl.col("travel_time").cum_sum().over("trip_id")
                )
                .with_columns(entry_time=pl.col("exit_time") - pl.col("travel_time"))
                .select("trip_id", "edge_id", "entry_time", "exit_time", "travel_time")
            )
            # Note. `how="vertical_relaxed"` is required when edge ids have been converted to String
            # in the simulation (e.g., in case of HOV edges), while original edge ids are of integer
            # type.
            lf = pl.concat((lf, secondary_routes), how="vertical_relaxed", rechunk=True)
        df = lf.sort("trip_id", "entry_time").collect()
        self.output["route_results"].write(df)


class ActivityResultsStep(PopulationStep):
    """Reads the results from the Metropolis-Core simulation and produces a clean file for activity
    results.
    """

    input_files = {"trips": TripsFile, "trip_results": TripResultsFile}
    output_files = {"activity_results": ActivityResultsFile}

    def run(self):
        import polars as pl

        trips: pl.DataFrame = self.input["trips"].read()
        for x in ("origin", "destination"):
            col = f"{x}_purpose_group"
            if col not in trips.columns:
                trips = trips.with_columns(pl.lit(None, dtype=pl.String).alias(col))
        trips = trips.select(
            "person_id", "trip_id", "origin_purpose_group", "destination_purpose_group"
        )
        first_activities = trips.group_by("person_id").agg(
            preceding_trip_id=pl.lit(None, dtype=trips.schema["trip_id"]),
            following_trip_id=pl.col("trip_id").first(),
            purpose=pl.col("origin_purpose_group").first(),
        )
        other_activities = trips.select(
            "person_id",
            preceding_trip_id="trip_id",
            following_trip_id=pl.col("trip_id").shift(-1).over("person_id"),
            purpose="destination_purpose_group",
        )
        activities = pl.concat((first_activities, other_activities), how="vertical")
        trip_results = (
            self.input["trip_results"]
            .scan()
            .select("trip_id", "departure_time", "arrival_time")
            .collect()
        )
        # Add activity start time from arrival time of preceding trip.
        activities = activities.join(
            trip_results.select(preceding_trip_id="trip_id", start_time="arrival_time"),
            on="preceding_trip_id",
            how="left",
        )
        # Add activity end time from departure time of following trip.
        activities = activities.join(
            trip_results.select(following_trip_id="trip_id", end_time="departure_time"),
            on="following_trip_id",
            how="left",
        )
        activities = activities.with_columns(
            activity_duration=pl.col("end_time") - pl.col("start_time")
        )
        activities = activities.sort("person_id", "start_time")
        self.output["activity_results"].write(activities)


class TourResultsStep(PopulationStep):
    """Reads the trip-level results and the Metropolis-Core agent results and produces a clean file
    for results at the tour level.
    """

    input_files = {"trip_results": TripResultsFile, "metro_agent_results": MetroAgentResultsFile}
    output_files = {"tour_results": TourResultsFile}

    def run(self):
        import polars as pl

        prefix = f"{self.population_name}-"
        # In the main simulation, 1 agent = 1 tour.
        agent_results: pl.DataFrame = (
            self.input["metro_agent_results"]
            .scan()
            .filter(pl.col("agent_id").str.starts_with(prefix))
            .select(
                tour_id=pl.col("agent_id").str.strip_prefix(prefix),
                mode="selected_alt_id",
                total_utility="utility",
                mode_expected_utility="alt_expected_utility",
                expected_utility="expected_utility",
            )
            .collect()
        )
        tour_trips: pl.DataFrame = (
            self.input["trip_results"]
            .scan()
            .group_by("tour_id")
            .agg(
                tour_departure_time=pl.col("departure_time").min(),
                tour_arrival_time=pl.col("arrival_time").max(),
                total_travel_time=pl.col("travel_time").sum(),
                total_travel_utility=pl.col("travel_utility").sum(),
                total_schedule_utility=pl.col("schedule_utility").sum(),
            )
            .collect()
        )
        # Left join so that tours without any trip (e.g., outside option) are kept, with null
        # trip-based values.
        df = agent_results.join(tour_trips, on="tour_id", how="left").sort("tour_id")
        self.output["tour_results"].write(df)
