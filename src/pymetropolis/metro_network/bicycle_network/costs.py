from pymetropolis.metro_pipeline import Step

from .files import BicycleEdgesCleanFile, BicycleEdgesCostsFile


class ExogenousBicycleEdgeCostsStep(Step):
    """Generates costs for the bicycle network edges, from exogenous values."""

    input_files = {"clean_edges": BicycleEdgesCleanFile}
    output_files = {"edges_costs": BicycleEdgesCostsFile}

    priority = 0

    def run(self):
        import polars as pl

        edges = self.input["clean_edges"].read()
        df = pl.from_pandas(
            edges.loc[
                :,
                [
                    "edge_id",
                    "length",
                    "edge_type",
                    "quality",
                    "type",
                    "has_bump",
                    "traffic_signals",
                    "stop",
                    "give_way",
                ],
            ]
        )

        # df = df.select(
        #     "edge_id",
        #     # Constant penalties.
        #     cost=5.0
        #     + pl.col("traffic_signals") * 60.0
        #     + pl.col("stop") * 30.0
        #     + pl.col("give_way") * 10.0
        #     + pl.col("has_bump") * 10.0
        #     + pl.col("type").eq("foot") * 40.0
        #     + pl.col("type").eq("link") * 30.0
        #     + pl.col("type").eq("crossing") * 60.0
        #     # Quality penalties (each reduction of quality by 1 over 1k is equivalent to 60 seconds
        #     # lost).
        #     + pl.col("length") * (10 - pl.col("quality")) * 60.0 / 1000.0
        #     # Type penalties.
        #     + (pl.col("length") / 1000.0)
        #     * (
        #         pl.col("type").eq("foot") * 600.0
        #         + pl.col("type").eq("shared_track") * 100.0
        #         + pl.col("type").eq("opposite") * 60.0
        #         + pl.col("type").eq("busway") * 10.0
        #     )
        #     # Road penalties.
        #     + (pl.col("length") / 1000.0)
        #     * pl.col("type").is_in(("road", "shared_road", "lane"))
        #     * (
        #         pl.col("edge_type").eq("residential") * 10.0
        #         + pl.col("edge_type").eq("unclassified") * 15.0
        #         + pl.col("edge_type").eq("living_street") * 30.0
        #         + pl.col("edge_type").eq("service") * 60.0
        #         + pl.col("edge_type").eq("tertiary") * 15.0
        #         + pl.col("edge_type").eq("tertiary_link") * 15.0
        #         + pl.col("edge_type").eq("secondary") * 100.0
        #         + pl.col("edge_type").eq("secondary_link") * 100.0
        #         + pl.col("edge_type").eq("primary") * 200.0
        #         + pl.col("edge_type").eq("primary_link") * 200.0
        #     )
        #     * (0.7 * pl.col("type").eq("shared_road"))
        #     * (0.1 * pl.col("type").eq("lane")),
        # )

        df = df.select(
            "edge_id",
            cost=pl.col("length") / 1e3
            + 10.0
            * (
                pl.col("quality").lt(7)
                | pl.col("traffic_signals")
                | pl.col("stop")
                | pl.col("type").is_in(("foot", "crossing"))
                | pl.col("edge_type")
                .is_in(("primary", "primary_link"))
                .and_(pl.col("type") == "road")
            )
            + 1.0
            * (
                pl.col("quality").eq(7)
                | pl.col("give_way")
                | pl.col("type").is_in(("opposite", "shared_track"))
                | pl.col("edge_type")
                .is_in(("primary", "primary_link"))
                .and_(pl.col("type") == "shared_road")
                | pl.col("edge_type")
                .is_in(("secondary", "secondary_link", "service"))
                .and_(pl.col("type") == "road")
            ),
        )

        self.output["edges_costs"].write(df)
