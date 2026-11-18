from __future__ import annotations

from loguru import logger

from pymetropolis.metro_calibration.road.files import TargetRoutesFile
from pymetropolis.metro_calibration.road.utils import check_free_flow_tt
from pymetropolis.metro_common.plots import PlotStep, plot_travel_time_comparison

from .files import CongestionTimeComparisonPlotFile, TargetRoutesCongestionTimesFile


class CongestionTimeComparisonStep(PlotStep):
    """Compares observed and Metropolis-simulated congested times of the target routes."""

    input_files = {"routes": TargetRoutesFile, "congestion_times": TargetRoutesCongestionTimesFile}
    output_files = {"comparison_plot": CongestionTimeComparisonPlotFile}

    def run(self):
        import polars as pl

        routes = self.input["routes"].read()
        check_free_flow_tt(routes.columns)
        congestion_times = self.input["congestion_times"].scan()

        observed_tt = routes.select(
            "route_id",
            target_congested_time=(
                pl.col("target_tt") - pl.col("target_free_flow_tt")
            ).dt.total_seconds(fractional=True),
        )

        simulated_tt = congestion_times.select(
            "route_id",
            metropolis_congested_time=pl.col("congested_time").dt.total_seconds(fractional=True),
        ).collect()

        df = simulated_tt.join(observed_tt, on="route_id", how="inner")
        observed = df["target_congested_time"].to_numpy()
        predicted = df["metropolis_congested_time"].to_numpy()
        rmse = float(((observed - predicted) ** 2).mean() ** 0.5)

        logger.debug(f"Average congested time (observed): {observed.mean()}")
        logger.debug(
            f"Congested time (observed): {observed_tt['target_congested_time'].describe()}"
        )
        logger.debug(f"Average congested time (simulated): {predicted.mean()}")
        logger.debug(
            f"Congested time (simulated): {simulated_tt['metropolis_congested_time'].describe()}"
        )
        logger.debug(f"RMSE: {rmse:.2f}")

        fig = plot_travel_time_comparison(
            observed,
            predicted,
            rmse,
            xlabel="Observed congested time",
            ylabel="Metropolis congested time",
        )
        self.output["comparison_plot"].write(fig)
