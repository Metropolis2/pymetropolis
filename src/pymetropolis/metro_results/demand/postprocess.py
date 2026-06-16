from pymetropolis.metro_demand.population.files import TripsFile
from pymetropolis.metro_demand.routing.files import PrimaryCarTripsAccessEgressFile
from pymetropolis.metro_network.road_network.files import RoadEdgesFreeFlowTravelTimeFile
from pymetropolis.metro_pipeline.steps import InputFile, Step
from pymetropolis.metro_simulation.demand.files import MetroTripsFile
from pymetropolis.metro_simulation.run import MetroAgentResultsFile, MetroTripResultsFile
from pymetropolis.metro_simulation.run.files import MetroRouteResultsFile

from .files import ActivityResultsFile, RouteResultsFile, TripResultsFile


class TripResultsStep(Step):
    """Reads the results from the Metropolis-Core simulation and produces a clean file for results
    at the trip level.
    """

    input_files = {
        "metro_input_trips": MetroTripsFile,
        "metro_trip_results": MetroTripResultsFile,
        "metro_agent_results": MetroAgentResultsFile,
        "access_egress_parts": InputFile(PrimaryCarTripsAccessEgressFile, optional=True),
    }
    output_files = {"trip_results": TripResultsFile}

    def run(self):
        import polars as pl

        trip_results: pl.DataFrame = self.input["metro_trip_results"].read()
        agent_results: pl.DataFrame = self.input["metro_agent_results"].read()
        df = trip_results.join(
            agent_results.select("agent_id", "selected_alt_id"), on="agent_id", how="left"
        )
        df = df.select(
            "trip_id",
            mode="selected_alt_id",
            is_road=pl.col("length").is_not_null(),
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
        if self.input["access_egress_parts"].exists():
            access_egress = self.input["access_egress_parts"].read()
            # Restrict access / egress to road trips.
            access_egress = access_egress.join(df.filter("is_road"), on="trip_id", how="semi")
            # Add access / egress values.
            # Note that the constant is already included in the travel utility so it should already
            # account for the access / egress part.
            df = (
                df.join(access_egress, on="trip_id", how="left")
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
                .drop(set(access_egress.columns) - {"trip_id"})
            )
        # Add vehicle_id.
        input_trips = self.input["metro_input_trips"].read()
        df = df.join(
            input_trips.select("trip_id", mode="alt_id", vehicle_id="class.vehicle"),
            on=["trip_id", "mode"],
            how="left",
        )
        df = df.with_columns(travel_time=pl.col("arrival_time") - pl.col("departure_time"))
        self.output["trip_results"].write(df)


class RouteResultsStep(Step):
    """Reads the results from the Metropolis-Core simulation and produces a clean file for route
    results of road trips.
    """

    input_files = {
        "metro_route_results": MetroRouteResultsFile,
        "access_egress_parts": InputFile(PrimaryCarTripsAccessEgressFile, optional=True),
        "edges_fftt": RoadEdgesFreeFlowTravelTimeFile,
    }
    output_files = {"route_results": RouteResultsFile}

    def run(self):
        import polars as pl

        df: pl.DataFrame = (
            self.input["metro_route_results"]
            .scan()
            .select("trip_id", "edge_id", "entry_time", "exit_time")
            .collect()
        )
        lf = df.lazy()
        # Clean entry / exit time and add travel time.
        lf = lf.with_columns(
            entry_time=pl.duration(seconds="entry_time"), exit_time=pl.duration(seconds="exit_time")
        ).with_columns(travel_time=pl.col("exit_time") - pl.col("entry_time"))
        # Add access / egress part.
        if self.input["access_egress_parts"].exists():
            trip_timings: pl.DataFrame = (
                lf.group_by("trip_id")
                .agg(
                    primary_start=pl.col("entry_time").first(),
                    primary_end=pl.col("exit_time").last(),
                )
                .collect()
            )  # ty: ignore[invalid-assignment]
            access_egress: pl.LazyFrame = self.input["access_egress_parts"].scan()
            edges_fftt: pl.LazyFrame = (
                self.input["edges_fftt"].scan().rename({"free_flow_travel_time": "travel_time"})
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
            lf = pl.concat((lf, access_edges, egress_edges), how="vertical")
        df = lf.sort("trip_id", "entry_time").collect()  # ty: ignore[invalid-assignment]
        self.output["route_results"].write(df)


class ActivityResultsStep(Step):
    """Reads the results from the Metropolis-Core simulation and produces a clean file for activity
    results.
    """

    input_files = {"trips": TripsFile, "trip_results": TripResultsFile}
    output_files = {"activity_results": ActivityResultsFile}

    def run(self):
        import polars as pl

        trips: pl.DataFrame = (
            self.input["trips"]
            .scan()
            .select("person_id", "trip_id", "origin_purpose_group", "destination_purpose_group")
            .collect()
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
        # Add activity end time from departure time of following trip.
        activities = activities.join(
            trip_results.select(following_trip_id="trip_id", end_time="departure_time"),
            on="following_trip_id",
            how="left",
        )
        # Add activity start time from arrival time of preceding trip.
        activities = activities.join(
            trip_results.select(preceding_trip_id="trip_id", start_time="arrival_time"),
            on="preceding_trip_id",
            how="left",
        )
        activities = activities.with_columns(
            activity_duration=pl.col("end_time") - pl.col("start_time")
        )
        activities = activities.sort("person_id", "start_time")
        self.output["activity_results"].write(activities)
