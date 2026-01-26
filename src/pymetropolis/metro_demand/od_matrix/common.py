import numpy as np
import polars as pl


def generate_trips_from_od_matrix(df: pl.DataFrame, rng: np.random.Generator):
    if df["size"].dtype.is_float():
        decimals = df["size"] % 1.0
        if (decimals != 0.0).any():
            # Some values for `size` are not integers: we randomly draw the previous or next integer
            # for each value, with probability equal to the decimal part.
            # The draws are such that, on aggregate, the total number of trips is equal to the sum
            # of `size`.
            nb_extras = decimals.sum()
            nb_extras = int(nb_extras) + rng.binomial(1, nb_extras % 1.0)
            extras = rng.choice(
                len(df), size=nb_extras, replace=False, p=decimals.to_numpy() / decimals.sum()
            )
            df = df.with_columns(
                index=pl.arange(pl.len()), int_size=pl.col("size").cast(pl.UInt32)
            ).with_columns(
                size=pl.when(pl.col("index").is_in(extras))
                .then(pl.col("size") + 1)
                .otherwise("size")
            )
    trips = pl.DataFrame(
        {
            "origin_node_id": np.repeat(df["origin"], df["size"]),
            "destination_node_id": np.repeat(df["destination"], df["size"]),
        }
    )
    trips = trips.with_columns(trip_id=pl.arange(1, pl.len() + 1, dtype=pl.UInt64))
    return trips
