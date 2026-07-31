from pymetropolis.metro_common.routing import compute_all_pairs_dijkstra
from pymetropolis.metro_common.utils import pl_duration_to_seconds
from pymetropolis.metro_network.road_network.files import RoadEdgesCleanFile
from pymetropolis.metro_pipeline import Step

from .files import AllRoadFreeFlowTravelTimesFile, RoadEdgesFreeFlowTravelTimeFile


class AllFreeFlowTravelTimesStep(Step):
    """Computes travel time of the fastest path under (car) free-flow conditions, for all node pairs
    of the road network.
    """

    input_files = {"edges": RoadEdgesCleanFile, "edges_fftt": RoadEdgesFreeFlowTravelTimeFile}
    output_files = {"all_free_flow_travel_times": AllRoadFreeFlowTravelTimesFile}
    priority = 0

    def run(self):
        import polars as pl

        edges_gdf = self.input["edges"].read()
        edges = pl.from_pandas(edges_gdf.loc[:, ["edge_id", "source", "target"]])
        edges_fftt = self.input["edges_fftt"].read()
        edges = edges.join(edges_fftt, on="edge_id").select(
            "source", "target", weight=pl_duration_to_seconds("free_flow_travel_time")
        )
        df = compute_all_pairs_dijkstra(edges)
        df = df.with_columns(free_flow_travel_time=pl.duration(seconds="weight")).drop("weight")
        self.output["all_free_flow_travel_times"].write(df)
