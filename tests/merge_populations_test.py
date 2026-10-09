import polars as pl

from pymetropolis.metro_simulation.common import merge_populations


class _File:
    def __init__(self, df: pl.DataFrame):
        self.df = df

    def read(self) -> pl.DataFrame:
        return self.df


def test_merge_populations_with_different_columns():
    main = pl.DataFrame({"agent_id": ["1"], "alt_choice.type": ["Logit"], "alt_choice.mu": [1.0]})
    trucks = pl.DataFrame({"agent_id": ["1"]})
    df = merge_populations({"population": _File(main), "trucks": _File(trucks)})  # ty: ignore[invalid-argument-type]
    assert df["agent_id"].to_list() == ["population-1", "trucks-1"]
    assert df["alt_choice.type"].to_list() == ["Logit", None]
    assert df["alt_choice.mu"].to_list() == [1.0, None]
