from pymetropolis.metro_common.routing import compute_all_pairs_dijkstra
from pymetropolis.metro_pipeline import Step

from .files import AllRoadDistancesFile, RoadEdgesCleanFile


class AllRoadDistancesStep(Step):
    """Computes distance of the shortest path, for all node pairs of the road network."""

    input_files = {"clean_edges": RoadEdgesCleanFile}
    output_files = {"all_distances": AllRoadDistancesFile}
    priority = 0

    def run(self):
        import polars as pl

        edges = self.input["clean_edges"].read()
        edges = pl.from_pandas(edges.loc[:, ["edge_id", "source", "target", "length"]])
        edges = edges.select("source", "target", weight="length")
        df = compute_all_pairs_dijkstra(edges)
        df = df.rename({"weight": "distance"})
        self.output["all_distances"].write(df)
