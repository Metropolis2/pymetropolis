import networkx as nx
import polars as pl

from pymetropolis.metro_pipeline import Step

from .files import AllDistancesFile, AllFreeFlowTravelTimesFile, CleanEdgesFile, EdgesPenaltiesFile


def compute_all_pairs_dijkstra(edges: pl.DataFrame) -> pl.DataFrame:
    dtype = edges["source"].dtype
    G = nx.DiGraph()
    G.add_weighted_edges_from(edges.iter_rows(), weight="weight")
    ods = list()
    for origin, data in nx.all_pairs_dijkstra_path_length(G, weight="weight"):
        for destination, weight in data.items():
            ods.append((origin, destination, weight))
    df = pl.DataFrame(
        ods,
        orient="row",
        schema={"origin_id": dtype, "destination_id": dtype, "weight": pl.Float64},
    )
    return df


class AllFreeFlowTravelTimesStep(Step):
    output_files = {"all_free_flow_travel_times": AllFreeFlowTravelTimesFile}

    def required_files(self):
        return {"clean_edges": CleanEdgesFile}

    def optional_files(self):
        return {"edges_penalties": EdgesPenaltiesFile}

    def run(self):
        edges = self.input["clean_edges"].read()
        edges = pl.from_pandas(
            edges.loc[:, ["edge_id", "source", "target", "length", "speed_limit"]]
        )
        if self.input["edges_penalties"].exists():
            penalties = self.input["edges_penalties"].read()
            edges = edges.join(penalties, on="edge_id", how="left")
        else:
            edges = edges.with_columns(constant=pl.lit(0.0, dtype=pl.Float64))
        edges = edges.select(
            "source",
            "target",
            tt=pl.col("length") / pl.col("speed_limit") * 3.6 + pl.col("constant"),
        )
        df = compute_all_pairs_dijkstra(edges)
        df = df.with_columns(free_flow_travel_time=pl.duration(seconds="weight")).drop("weight")
        self.output["all_free_flow_travel_times"].write(df)


class AllDistancesStep(Step):
    output_files = {"all_distances": AllDistancesFile}

    def required_files(self):
        return {"clean_edges": CleanEdgesFile}

    def run(self):
        edges = self.input["clean_edges"].read()
        edges = pl.from_pandas(edges.loc[:, ["edge_id", "source", "target", "length"]])
        edges = edges.select("source", "target", weight="length")
        df = compute_all_pairs_dijkstra(edges)
        df = df.rename({"weight": "distance"})
        self.output["all_distances"].write(df)
