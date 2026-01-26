import polars as pl

from pymetropolis.metro_demand.modes import CarDriverODsFile
from pymetropolis.metro_network.road_network import AllFreeFlowTravelTimesFile
from pymetropolis.metro_pipeline.parameters import FloatParameter, IntParameter
from pymetropolis.metro_pipeline.steps import MetroStep

from .common import generate_trips_from_od_matrix


class GravityODMatrixStep(MetroStep):
    exponential_decay = FloatParameter(
        "gravity_od_matrix.exponential_decay",
        description="Exponential decay rate of flows as a function of free-flow travel times (rate per minute)",
    )
    trips_per_node = IntParameter(
        "gravity_od_matrix.trips_per_node",
        description="Number of trips to be generated originating from each node",
    )
    output_files = {"car_driver_ods": CarDriverODsFile}

    def is_defined(self) -> bool:
        return self.exponential_decay is not None and self.trips_per_node is not None

    def required_files(self):
        return {"all_free_flow_travel_times": AllFreeFlowTravelTimesFile}

    def run(self):
        df = self.input["all_free_flow_travel_times"].read()
        df = df.filter(pl.col("origin_id") != pl.col("destination_id"))
        decay = self.exponential_decay
        df = df.with_columns(
            rate=(-pl.lit(decay) * pl.col("free_flow_travel_time").dt.total_seconds() / 60).exp()
        )
        df = df.with_columns(
            normalized_rate=pl.col("rate") / pl.col("rate").sum().over("origin_id")
        )
        df = df.with_columns(size=pl.col("normalized_rate") * self.trips_per_node)
        df = df.select(origin="origin_id", destination="destination_id", size="size")
        trips = generate_trips_from_od_matrix(df, self.get_rng())
        self.output["car_driver_ods"].write(trips)
