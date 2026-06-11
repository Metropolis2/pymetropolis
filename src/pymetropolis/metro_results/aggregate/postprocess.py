import json

from pymetropolis.metro_demand.population.files import TripsDistancesFile
from pymetropolis.metro_pipeline.steps import InputFile, Step
from pymetropolis.metro_results.demand.files import TripResultsFile
from pymetropolis.metro_simulation.common import StepWithSimulationRatio
from pymetropolis.metro_simulation.run import MetroIterationResultsFile
from pymetropolis.metro_simulation.supply.files import MetroVehicleTypesFile

from .files import AggregateOutputFile, IterationResultsFile


class AggregateResultsStep(StepWithSimulationRatio):
    """Generates a JSON file with various aggregate results on the simulation.

    NOTE: The developpement of this step is still in progress. More output will be added in the
    future. Do not hesitate to suggest additional output.

    Currently available output:

    - vehicle-kilometers (total, weighted by PCE, by mode)
    - mode shares (by trip count, by trip Euclidean distance)
    """

    input_files = {
        "metro_input_vehicles": MetroVehicleTypesFile,
        "trip_results": TripResultsFile,
        "trips_distances": InputFile(TripsDistancesFile, optional=True),
    }
    output_files = {"output": AggregateOutputFile}

    def run(self):
        import polars as pl

        results = dict()

        trip_results = self.input["trip_results"].read()
        vehicles = self.input["metro_input_vehicles"].read()
        trips_distances = self.input["trips_distances"].read_if_exists()

        # Compute vehicle-kilometers.
        results["vehicle_kilometers"] = dict()
        # Merge with vehicles to get PCE.
        trip_results = trip_results.join(vehicles, on="vehicle_id", how="left")
        results["vehicle_kilometers"]["total"] = (
            trip_results["route_length"].sum() / self.simulation_ratio / 1e3
        )
        # Note. We don't divide by the simulation ratio when computing veh km weighted by PCE since
        # the PCE already account for the simulation ratio.
        results["vehicle_kilometers"]["total_weighted_by_pce"] = trip_results.select(
            (pl.col("route_length") * pl.col("pce")).sum() / 1e3
        ).item()
        veh_km_by_mode = (
            trip_results.group_by("mode")
            .agg(veh_km=pl.col("route_length").sum() / self.simulation_ratio / 1e3)
            .filter(pl.col("veh_km") > 0.0)
            .sort("mode")
        )
        results["vehicle_kilometers"]["by_mode"] = {
            mode: veh_km for mode, veh_km in zip(veh_km_by_mode["mode"], veh_km_by_mode["veh_km"])
        }

        # Compute mode shares.
        results["mode_shares"] = dict()
        trip_count_shares = trip_results["mode"].value_counts(normalize=True).sort("mode")
        results["mode_shares"]["trip_count"] = {
            mode: share
            for mode, share in zip(trip_count_shares["mode"], trip_count_shares["proportion"])
        }
        if trips_distances is not None:
            trip_results = trip_results.join(trips_distances, on="trip_id")
            trip_length_shares = (
                trip_results.group_by("mode")
                .agg(pl.col("od_distance").sum())
                .with_columns(proportion=pl.col("od_distance") / pl.col("od_distance").sum())
                .sort("mode")
            )
            results["mode_shares"]["trip_euclidean_distance"] = {
                mode: share
                for mode, share in zip(trip_length_shares["mode"], trip_length_shares["proportion"])
            }

        # Save as JSON.
        results_str = json.dumps(results, indent=2, sort_keys=True)
        self.output["output"].write(results_str)


class IterationResultsStep(Step):
    """Reads the iteration-level results from the Metropolis-Core simulation and produces a clean
    file for the results.
    """

    input_files = {"metro_iteration_results": MetroIterationResultsFile}
    output_files = {"iteration_results": IterationResultsFile}

    def run(self):
        import polars as pl

        raw_results: pl.DataFrame = self.input["metro_iteration_results"].read()
        df = raw_results.with_columns(
            trip_count=pl.col("road_trip_count").fill_null(0)
            + pl.col("virtual_trip_count").fill_null(0)
        )
        df = df.select(
            iteration="iteration_counter",
            mean_surplus="surplus_mean",
            std_surplus="surplus_std",
            mean_tour_departure_time=pl.duration(seconds="alt_departure_time_mean"),
            mean_tour_arrival_time=pl.duration(seconds="alt_arrival_time_mean"),
            mean_tour_travel_time=pl.duration(seconds="alt_travel_time_mean"),
            mean_tour_simulated_utility="alt_utility_mean",
            mean_tour_expected_utility="alt_expected_utility_mean",
            mean_tour_departure_time_shift=pl.duration(seconds="alt_dep_time_shift_mean"),
            rmse_tour_departure_time=pl.duration(seconds="alt_dep_time_rmse"),
            # Note. We could add `nb_road_trips` and `nb_virtual_trips` as variables but road trips
            # on the secondary network only are not integrated in the road trips, which would be
            # confusing.
            nb_trips="trip_count",
            nb_outside_options=pl.col("no_trip_alt_count").fill_null(0),
            mean_trip_departure_time=pl.duration(
                seconds=(
                    pl.col("road_trip_departure_time_mean") * pl.col("road_trip_count")
                    + pl.col("virtual_trip_departure_time_mean") * pl.col("virtual_trip_count")
                )
                / pl.col("trip_count")
            ),
            mean_trip_arrival_time=pl.duration(
                seconds=(
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
            mean_road_trip_departure_time=pl.duration(seconds="road_trip_departure_time_mean"),
            mean_road_trip_arrival_time=pl.duration(seconds="road_trip_arrival_time_mean"),
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
