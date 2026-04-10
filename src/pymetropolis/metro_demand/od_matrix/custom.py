import polars as pl

from pymetropolis.metro_common.io import read_dataframe
from pymetropolis.metro_demand.modes.car import CarODsFile
from pymetropolis.metro_network.road_network.files import RoadEdgesCleanFile
from pymetropolis.metro_pipeline.parameters import PathParameter
from pymetropolis.random import RandomStep

from .common import generate_trips_from_od_matrix


class CustomODMatrixStep(RandomStep):
    """Generates car driver origin-destination pairs from the provided origin-destination matrix.

    The origin-destination matrix is provided in a CSV or Parquet file with the following columns:

    - `origin`: id of the origin node,
    - `destination`: id of the destination node,
    - `size`: number of trips to be generated from origin to destination.

    The `size` variable can be integers or floats.
    For float values, stochastic rounding is used to convert the value to an integer: e.g., 3.3 is
    converted to 3 with probability 70% and to 4 with probability 30%.
    """

    file = PathParameter(
        "od_matrix.file",
        check_file_exists=True,
        description="Path to the CSV or Parquet file containing the origin-destination matrix.",
        note=(
            "Required columns are: `origin` (id of origin node), `destination` (id of destination "
            "node), and `size` (int or float, number of trips)."
        ),
    )
    input_files = {"edges": RoadEdgesCleanFile}
    output_files = {"car_driver_ods": CarODsFile}

    def is_defined(self) -> bool:
        return self.file is not None

    def run(self):
        df = read_dataframe(self.file, columns=["origin", "destination", "size"])
        df = df.filter(pl.col("origin") != pl.col("destination"))
        trips = generate_trips_from_od_matrix(df, self.get_rng())
        self.output["car_driver_ods"].write(trips)
