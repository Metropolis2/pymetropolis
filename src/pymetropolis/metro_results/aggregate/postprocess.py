import polars as pl

from pymetropolis.metro_common.utils import seconds_since_midnight_to_time_pl
from pymetropolis.metro_pipeline.steps import Step
from pymetropolis.metro_simulation.run import MetroIterationResultsFile

from .files import IterationResultsFile


class IterationResultsStep(Step):
    """Reads the iteration-level results from the Metropolis-Core simulation and produces a clean
    file for the results.
    """

    input_files = {
        "metro_iteration_results": MetroIterationResultsFile,
    }
    output_files = {"iteration_results": IterationResultsFile}

    def run(self):
        raw_results: pl.DataFrame = self.input["metro_iteration_results"].read()
        df = raw_results.with_columns(
            trip_count=pl.col("road_trip_count") + pl.col("virtual_trip_count")
        )
        df = df.select(
            iteration="iteration_counter",
            mean_surplus="surplus_mean",
            std_surplus="surplus_std",
            mean_tour_departure_time=seconds_since_midnight_to_time_pl("alt_departure_time_mean"),
            mean_tour_arrival_time=seconds_since_midnight_to_time_pl("alt_arrival_time_mean"),
            mean_tour_travel_time=pl.duration(seconds="alt_travel_time_mean"),
            mean_tour_simulated_utility="alt_utility_mean",
            mean_tour_expected_utility="alt_expected_utility_mean",
            mean_tour_departure_time_shift=pl.duration(seconds="alt_dep_time_shift_mean"),
            rmse_tour_departure_time=pl.duration(seconds="alt_dep_time_rmse"),
            nb_road_trips=pl.col("road_trip_count").fill_null(0),
            nb_non_road_trips=pl.col("virtual_trip_count").fill_null(0),
            nb_outside_options=pl.col("no_trip_alt_count").fill_null(0),
            mean_trip_departure_time=seconds_since_midnight_to_time_pl(
                (
                    pl.col("road_trip_departure_time_mean") * pl.col("road_trip_count")
                    + pl.col("virtual_trip_departure_time_mean") * pl.col("virtual_trip_count")
                )
                / pl.col("trip_count")
            ),
            mean_trip_arrival_time=seconds_since_midnight_to_time_pl(
                (
                    pl.col("road_trip_arrival_time_mean") * pl.col("road_trip_count")
                    + pl.col("virtual_trip_arrival_time_mean") * pl.col("virtual_trip_count")
                )
                / pl.col("trip_count")
            ),
            mean_trip_travel_time=pl.duration(
                seconds=(
                    pl.col("road_trip_travel_time_mean") * pl.col("road_trip_count")
                    + pl.col("virtual_trip_travel_time_mean") * pl.col("virtual_trip_count")
                )
                / pl.col("trip_count")
            ),
            mean_trip_utility=(
                pl.col("road_trip_utility_mean") * pl.col("road_trip_count")
                + pl.col("virtual_trip_utility_mean") * pl.col("virtual_trip_count")
            )
            / pl.col("trip_count"),
            mean_road_trip_departure_time=seconds_since_midnight_to_time_pl(
                "road_trip_departure_time_mean"
            ),
            mean_road_trip_arrival_time=seconds_since_midnight_to_time_pl(
                "road_trip_arrival_time_mean"
            ),
            mean_road_trip_travel_time=pl.duration(seconds="road_trip_travel_time_mean"),
            mean_road_trip_route_free_flow_travel_time_mean=pl.duration(
                seconds="road_trip_route_free_flow_travel_time_mean"
            ),
            mean_road_trip_global_free_flow_travel_time=pl.duration(
                seconds="road_trip_global_free_flow_travel_time_mean"
            ),
            mean_road_trip_route_congestion_time=pl.duration(
                seconds=pl.col("road_trip_travel_time_mean")
                - pl.col("road_trip_route_free_flow_travel_time_mean")
            ),
            mean_road_trip_global_congestion_time=pl.duration(
                seconds=pl.col("road_trip_travel_time_mean")
                - pl.col("road_trip_global_free_flow_travel_time_mean")
            ),
            mean_road_trip_length="road_trip_length_mean",
            mean_road_trip_edge_count="road_trip_edge_count_mean",
            mean_road_trip_utility="road_trip_utility_mean",
            mean_road_trip_exp_travel_time=pl.duration(seconds="road_trip_exp_travel_time_mean"),
            mean_road_trip_exp_travel_time_abs_diff=pl.duration(
                seconds="road_trip_exp_travel_time_abs_diff_mean"
            ),
            rmse_road_trip_exp_travel_time_diff=pl.duration(
                seconds="road_trip_exp_travel_time_diff_rmse"
            ),
            mean_road_trip_length_diff="road_trip_length_diff_mean",
            rmse_simulated_road_travel_times=pl.duration(seconds="sim_road_network_cond_rmse"),
            rmse_expected_road_travel_times=pl.duration(seconds="exp_road_network_cond_rmse"),
        )
        self.output["iteration_results"].write(df)
