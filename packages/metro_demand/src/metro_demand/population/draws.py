import numpy as np
import polars as pl
from metro_common.errors import error_context
from metro_pipeline import RANDOM_SEED, Config, Step

from .files import TRIPS_FILE, UNIFORM_DRAWS_FILE


@error_context(msg="Cannot generate uniform draws for mode and departure-time choices.")
def draw(config: Config):
    trips = TRIPS_FILE.read(config)
    rng = np.random.default_rng(config[RANDOM_SEED])
    tour_ids = trips["tour_id"].unique().sort()
    nb_tours = len(tour_ids)
    mode_u = rng.random(size=nb_tours)
    dt_u = rng.random(size=nb_tours)
    df = pl.DataFrame({"tour_id": tour_ids, "mode_u": mode_u, "departure_time_u": dt_u})
    UNIFORM_DRAWS_FILE.save(df, config)
    return True


UNIFORM_DRAWS_STEP = Step(
    "uniform-draws", draw, required_files=[TRIPS_FILE], output_files=[UNIFORM_DRAWS_FILE]
)
