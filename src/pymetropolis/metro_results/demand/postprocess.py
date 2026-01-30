import polars as pl

from pymetropolis.metro_common.utils import seconds_since_midnight_to_time_pl
from pymetropolis.metro_pipeline.steps import Step
from pymetropolis.metro_simulation.run import MetroAgentResultsFile, MetroTripResultsFile

from .files import TripResultsFile


class TripResultsStep(Step):
    """Reads the results from the Metropolis-Core simulation and produces a clean file for results
    at the trip level.
    """

    input_files = {
        "metro_trip_results": MetroTripResultsFile,
        "metro_agent_results": MetroAgentResultsFile,
    }
    output_files = {"trip_results": TripResultsFile}

    def run(self):
        trip_results: pl.DataFrame = self.input["metro_trip_results"].read()
        agent_results: pl.DataFrame = self.input["metro_agent_results"].read()
        df = trip_results.join(
            agent_results.select("agent_id", "selected_alt_id"), on="agent_id", how="left"
        )
        df = df.select(
            "trip_id",
            mode="selected_alt_id",
            is_road=pl.col("length").is_not_null(),
            departure_time=seconds_since_midnight_to_time_pl("departure_time"),
            arrival_time=seconds_since_midnight_to_time_pl("arrival_time"),
            route_free_flow_travel_time=pl.duration(seconds="route_free_flow_travel_time"),
            global_free_flow_travel_time=pl.duration(seconds="global_free_flow_travel_time"),
            utility=pl.col("travel_utility") + pl.col("schedule_utility"),
            travel_utility="travel_utility",
            schedule_utility="schedule_utility",
            length="length",
            nb_edges="nb_edges",
        ).with_columns(travel_time=pl.col("arrival_time") - pl.col("departure_time"))
        self.output["trip_results"].write(df)
