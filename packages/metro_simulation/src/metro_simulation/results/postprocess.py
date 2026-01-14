import polars as pl
from metro_common.errors import error_context
from metro_common.utils import seconds_since_midnight_to_time_pl
from metro_pipeline import Config, Step

from metro_simulation.run import METRO_AGENT_RESULTS_FILE, METRO_TRIP_RESULTS_FILE

from .files import TRIP_RESULTS_FILE


@error_context(msg="Cannot write trip results file")
def write_trip_results(config: Config):
    trip_results = METRO_TRIP_RESULTS_FILE.read(config)
    agent_results = METRO_AGENT_RESULTS_FILE.read(config)
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
    TRIP_RESULTS_FILE.save(df, config)
    return True


TRIP_RESULTS_STEP = Step(
    "trip-results",
    write_trip_results,
    required_files=[METRO_TRIP_RESULTS_FILE, METRO_AGENT_RESULTS_FILE],
    output_files=[TRIP_RESULTS_FILE],
)
