from datetime import time, timedelta
from typing import Any, Optional

import numpy as np
import polars as pl

from pymetropolis.metro_common.errors import MetropyError, error_context
from pymetropolis.metro_common.utils import (
    seconds_since_midnight_to_time_pl,
    time_to_seconds_since_midnight,
)
from pymetropolis.metro_pipeline.parameters import IntParameter, Parameter
from pymetropolis.metro_pipeline.steps import Step
from pymetropolis.metro_pipeline.types import CustomValidator, Duration, Float, Int, Time, Type


class RandomStep(Step):
    """A Step subclass for Steps that make use of random number generation."""

    random_seed = IntParameter(
        "random_seed",
        description="Random seed used to initialize the random number generator.",
        note=(
            "If the random seed is not defined, some operations are not deterministic, i.e., they can "
            "produce different results if re-run."
        ),
    )

    def get_rng(self) -> np.random.Generator:
        return np.random.default_rng(self.random_seed)


# List of valid `distribution` values.
DISTRIBUTIONS = ["Uniform", "Gaussian", "Normal", "Lognormal"]
# Put the values in lowercase so that comparison ignore case.
DISTRIBUTIONS_LOWER = [d.lower() for d in DISTRIBUTIONS]


@error_context("Not a valid distribution parameter")
def validate_distribution(value: Any, inner: Type, inner_mean: Type, inner_std: Type) -> Any:
    """Validates that the given value is a correctly specified distribution.

    If the value is a dictionary, validates that it has keys "mean", "std", and "distribution".
    Value "mean" must be a valid `inner_mean` type.
    Value "std" must be a valid `inner_std` type.
    Value "distribution" must be a valid distribution name.

    Otherwise, the value must be a valid `inner` type.
    """
    if isinstance(value, dict):
        if "mean" not in value:
            raise MetropyError("Missing key `mean`")
        if "std" not in value:
            raise MetropyError("Missing key `std`")
        if "distribution" not in value:
            raise MetropyError("Missing key `distribution`")
        value["mean"] = inner_mean.validate(value["mean"])
        value["std"] = inner_std.validate(value["std"])
        if value["distribution"].lower() not in DISTRIBUTIONS_LOWER:
            raise MetropyError(f"Not a supported distribution: `{value['distribution']}`")
        return value
    else:
        # Supposedly a constant distribution.
        # Return the value itself if it passes the validator.
        return inner.validate(value)


class DistributionParameter(Parameter[Any]):
    """Special parameter type for values that can be distributed.

    The parameter can be specified either as a constant value or as a table with keys "mean", "std",
    and "distribution".

    The constant value must be valid with respect to the `inner` type.
    The mean value must be valid with respect to the `inner_mean` type (equal to `inner` by
    default).
    The standard-deviation must be valid with respect to the `inner_std` type (equal to `inner` by
    default).
    """

    def __init__(
        self,
        *args,
        inner: Type,
        inner_mean: Optional[Type] = None,
        inner_std: Optional[Type] = None,
        **kwargs,
    ):
        if inner_mean is None:
            inner_mean = inner
        if inner_std is None:
            inner_std = inner
        distrs = ", ".join(map(lambda d: f"`{repr(d)}`", DISTRIBUTIONS))
        kwargs["validator"] = CustomValidator(
            fn=lambda v: validate_distribution(
                v, inner=inner, inner_mean=inner_mean, inner_std=inner_std
            ),
            description=(
                f"{inner._describe()} or a table with keys `mean` ({inner_mean._describe()}), "
                f"`std` ({inner_std._describe()}), and `distribution` (one of {distrs})"
            ),
        )
        super().__init__(*args, **kwargs)


class FloatDistributionParameter(DistributionParameter):
    """Special parameter type for Float values that can be distributed."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, inner=Float(), **kwargs)


class IntDistributionParameter(DistributionParameter):
    """Special parameter type for Int values that can be distributed.

    The parameter is either a constant integer or a distribution (with floats mean and std).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, inner=Int(), inner_mean=Float(), inner_std=Float(), **kwargs)


class TimeDistributionParameter(DistributionParameter):
    """Special parameter type for Time values that can be distributed."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, inner=Time(), inner_std=Duration(), **kwargs)


class DurationDistributionParameter(DistributionParameter):
    """Special parameter type for Duration values that can be distributed."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, inner=Duration(), inner_std=Duration(), **kwargs)


@error_context("Failed to generate values from the given distribution")
def generate_values(param: Any, n: int, rng: np.random.Generator) -> pl.Series:
    if isinstance(param, dict):
        distr = param["distribution"].lower()
        mean = float(param["mean"])
        std = float(param["std"])
        if distr == "uniform":
            values = rng.uniform(mean - std, mean + std, size=n)
        elif distr in ("gaussian", "normal"):
            values = rng.normal(mean, scale=std, size=n)
        elif distr == "lognormal":
            values = rng.lognormal(mean, sigma=std, size=n)
        else:
            raise MetropyError(f"Unsupported distribution: {distr}")
        return pl.Series(values)
    else:
        # Constant value.
        return pl.repeat(param, n, eager=True)


def generate_int_values(param: Any, n: int, rng: np.random.Generator) -> pl.Series:
    values = generate_values(param, n, rng)
    return values.round().cast(pl.Int64)


def generate_time_values(param: Any, n: int, rng: np.random.Generator) -> pl.Series:
    if isinstance(param, dict):
        float_param = param.copy()
        float_param["mean"] = float(time_to_seconds_since_midnight(param["mean"]))
        float_param["std"] = float(param["std"].total_seconds())
    else:
        # Constant value.
        assert isinstance(param, time)
        float_param = float(time_to_seconds_since_midnight(param))
    values = generate_values(float_param, n, rng)
    # Convert back to Time, through a DataFrame.
    df = pl.DataFrame({"value": values})
    return df.select(seconds_since_midnight_to_time_pl("value")).to_series()


def generate_duration_values(param: Any, n: int, rng: np.random.Generator) -> pl.Series:
    if isinstance(param, dict):
        float_param = param.copy()
        float_param["mean"] = float(param["mean"].total_seconds())
        float_param["std"] = float(param["std"].total_seconds())
    else:
        # Constant value.
        assert isinstance(param, timedelta)
        param = float(param.total_seconds())
    values = generate_values(param, n, rng)
    # Convert back to Timedelta, through a DataFrame.
    df = pl.DataFrame({"value": values})
    return df.select(pl.duration(seconds="value")).to_series()
