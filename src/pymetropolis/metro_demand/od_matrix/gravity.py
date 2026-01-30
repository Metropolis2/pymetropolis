import polars as pl

from pymetropolis.metro_demand.modes import CarDriverODsFile
from pymetropolis.metro_network.road_network import AllFreeFlowTravelTimesFile
from pymetropolis.metro_pipeline.parameters import FloatParameter, IntParameter, StringParameter
from pymetropolis.metro_pipeline.steps import RandomStep

from .common import generate_trips_from_od_matrix


class GravityODMatrixStep(RandomStep):
    r"""Generates car driver origin-destination pairs by generating trips from a gravity model.

    The model is based on de Palma, A., Kilani, M., & Lindsey, R. (2005). Congestion pricing on a
    road network: A study using the dynamic equilibrium simulator METROPOLIS. _Transportation
    Research Part A: Policy and Practice, 39_(7-9), 588-611.

    The total number of trips generated from each node is fixed (parameter `trips_per_node`).
    Then, the number of trips generated from node \\(i\\) to node \\(j\\) is proportional to:

    \\[ e^{-\\lambda \\cdot {tt}^0} \\]

    where \\(\\lambda\\) is the decay rate (parameter `exponential_decay`) and \\({tt}^0\\) is the
    free-flow travel time by car from node \\(i\\) to node \\(j\\).
    """

    exponential_decay = FloatParameter(
        "gravity_od_matrix.exponential_decay",
        description="Exponential decay rate of flows as a function of free-flow travel times (rate per minute)",
    )
    trips_per_node = IntParameter(
        "gravity_od_matrix.trips_per_node",
        description="Number of trips to be generated originating from each node",
    )
    nodes_regex = StringParameter(
        "gravity_od_matrix.nodes_regex",
        description="Regular expression specifying the nodes to be selected as possible origin / destination.",
        note="If not specified, any node can be an origin / destination.",
    )
    input_files = {"all_free_flow_travel_times": AllFreeFlowTravelTimesFile}
    output_files = {"car_driver_ods": CarDriverODsFile}

    def is_defined(self) -> bool:
        return self.exponential_decay is not None and self.trips_per_node is not None

    def run(self):
        df = self.input["all_free_flow_travel_times"].read()
        df = df.filter(pl.col("origin_id") != pl.col("destination_id"))
        if self.nodes_regex is not None:
            df = df.filter(
                pl.col("origin_id").str.contains(self.nodes_regex),
                pl.col("destination_id").str.contains(self.nodes_regex),
            )
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
