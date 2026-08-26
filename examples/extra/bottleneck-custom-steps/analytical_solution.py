"""Analytical equilibrium departure rate for the stochastic bottleneck model of de Palma,
Ben-Akiva, Lefevre, and Litinas (1983), ported from
https://github.com/lucasjavaudin/MetropolisBottleneck (`python/theoretical_solution.py`).

The functions below assume a single, homogeneous population (constant `alpha`, `beta`, `gamma`,
`tstar`, `delta`), which matches the bottleneck examples' config. `params` is a dict with keys
`n`, `mu`, `bottleneck_flow`, `alpha`, `beta`, `gamma`, `tstar`, `delta`, `period`, `tt0`, all in
consistent seconds-based units (see `DepartureRateComparisonStep` in `custom_steps.py`).
"""

import numpy as np
from scipy import optimize


def dep_rate(t, denominator, times, params):
    """Returns the analytical departure rate from origin at time `t`, given the value of the
    denominator `E` (equation 11), the time thresholds and the parameters.
    """
    # If `t_hat` is not in `times`, it is equal to `t_tilde`.
    t_hat = times.get("t_hat", times["t_tilde"])
    if t <= times["t_start"]:
        theta = 1.0 * (t + params["tt0"] <= params["tstar"] - params["delta"]) - (
            params["gamma"] / params["beta"]
        ) * (t + params["tt0"] >= params["tstar"] + params["delta"])
        to_be_exped = (
            params["beta"]
            * (np.abs(theta) * params["delta"] - theta * (params["tstar"] - params["tt0"]))
            / params["mu"]
        )
        if to_be_exped < -700.0:
            # Underflow.
            r = 0
        else:
            a = (params["n"] / denominator) * np.exp(to_be_exped)
            r = a * np.exp(theta * params["beta"] * t / params["mu"])
    elif t <= times["t_tilde"]:
        dep_rate_t_start = params["bottleneck_flow"]
        k = (
            1 / dep_rate_t_start
            - (params["alpha"] - params["beta"]) / (params["alpha"] * params["bottleneck_flow"])
        ) * np.exp(params["alpha"] * (times["t_start"] - t) / params["mu"])
        r = (
            params["alpha"]
            * params["bottleneck_flow"]
            / ((params["alpha"] - params["beta"]) + params["alpha"] * params["bottleneck_flow"] * k)
        )
    elif t <= t_hat:
        k = (
            1 / dep_rate(times["t_tilde"], denominator, times, params)
            - 1 / params["bottleneck_flow"]
        ) * np.exp(params["alpha"] * (times["t_tilde"] - t) / params["mu"])
        r = params["bottleneck_flow"] / (1 + params["bottleneck_flow"] * k)
    elif t <= times["t_end"]:
        k = (
            1 / dep_rate(t_hat, denominator, times, params)
            - (params["alpha"] + params["gamma"]) / (params["alpha"] * params["bottleneck_flow"])
        ) * np.exp(params["alpha"] * (t_hat - t) / params["mu"])
        r = (
            params["alpha"]
            * params["bottleneck_flow"]
            / (
                (params["alpha"] + params["gamma"])
                + params["alpha"] * params["bottleneck_flow"] * k
            )
        )
    else:
        r = (params["n"] / denominator) * np.exp(
            params["gamma"] * (params["tstar"] + params["delta"] - t - params["tt0"]) / params["mu"]
        )
    return max(r, 0.0)


def queue_length(t, denominator, times, params):
    """Returns the analytical queue length of the bottleneck at time `t` (equation 51)."""
    # If `t_hat` is not in `times`, it is equal to `t_tilde`.
    t_hat = times.get("t_hat", times["t_tilde"])
    if t <= times["t_start"]:
        # Congestion has not started.
        return 0.0
    elif t <= times["t_tilde"]:
        r = dep_rate(t, denominator, times, params)
        r_t_start = params["bottleneck_flow"]
        return (params["mu"] * params["bottleneck_flow"] / (params["alpha"] - params["beta"])) * (
            params["beta"] / params["mu"] * (t - times["t_start"]) - np.log(r / r_t_start)
        )
    elif t <= t_hat:
        d0 = queue_length(times["t_tilde"], denominator, times, params)
        r = dep_rate(t, denominator, times, params)
        r_t_tilde = dep_rate(times["t_tilde"], denominator, times, params)
        return d0 - (params["mu"] * params["bottleneck_flow"] / params["alpha"]) * np.log(
            r / r_t_tilde
        )
    elif t <= times["t_end"]:
        d0 = queue_length(t_hat, denominator, times, params)
        r = dep_rate(t, denominator, times, params)
        r_t_hat = dep_rate(t_hat, denominator, times, params)
        return d0 + (
            params["mu"] * params["bottleneck_flow"] / (params["alpha"] + params["gamma"])
        ) * (-params["gamma"] / params["mu"] * (t - t_hat) - np.log(r / r_t_hat))
    else:
        # Congestion is finished.
        return 0.0


def equilibrium_conditional(denominator, params):
    """Returns the time thresholds (`t_start`, `t_tilde`, `t_hat`, `t_end`) given the value of
    `E`.
    """
    times = {
        "t_start": params["period"][1],
        "t_tilde": params["tstar"] - params["delta"] - params["tt0"],
        "t_hat": params["tstar"] + params["delta"] - params["tt0"],
        "t_end": params["period"][1],
    }

    if params["n"] / denominator <= params["bottleneck_flow"]:
        # Congestion level is never reached.
        return times

    # `t_start` is the time at which the congestion starts, i.e., the departure rate goes above
    # the bottleneck capacity. We find `t_start` by inverting `r(t) = s`, using equation (27).
    a = (params["n"] / denominator) * np.exp(
        max(
            params["beta"] * (params["tt0"] - params["tstar"] + params["delta"]) / params["mu"],
            -700,
        )
    )
    t_start = (params["mu"] / params["beta"]) * np.log(params["bottleneck_flow"] / a)

    if t_start < params["period"][0]:
        # Congestion starts immediately.
        t_start = params["period"][0]

    times["t_start"] = t_start

    if t_start < times["t_tilde"]:
        # `t_tilde` is the earliest on-time departure (equation 14).
        times["t_tilde"] = optimize.bisect(
            lambda t: (
                t
                + params["tt0"]
                + queue_length(t, denominator, times, params) / params["bottleneck_flow"]
                - params["tstar"]
                + params["delta"]
            ),
            times["t_start"] - 1.0,
            times["t_tilde"] + 1.0,
        )
    # `t_hat` is the latest on-time departure (equation 15).
    if params["delta"] > 0:
        times["t_hat"] = optimize.bisect(
            lambda t: (
                t
                + params["tt0"]
                + queue_length(t, denominator, times, params) / params["bottleneck_flow"]
                - params["tstar"]
                - params["delta"]
            ),
            times["t_tilde"],
            params["period"][1],
        )
    else:
        times["t_hat"] = times["t_tilde"]

    # `t_end` is the time at which congestion ends, i.e., the time at which queue length reaches
    # zero again.
    if queue_length(params["period"][1], denominator, times, params) <= 0.0:
        times["t_end"] = optimize.bisect(
            lambda t: queue_length(t, denominator, times, params),
            times["t_tilde"],
            params["period"][1],
        )

    if times["t_hat"] == times["t_tilde"]:
        # `t_hat` is not required if it is equal to `t_tilde`.
        times.pop("t_hat")
    return times


def integral(t, times, denominator, params):
    """Returns the integral of the analytical departure rate from the start of the period to `t`,
    i.e., the cumulative number of departures.
    """
    # If `t_hat` is not in `times`, it is equal to `t_tilde`.
    t_hat = times.get("t_hat", times["t_tilde"])
    # Case A: t_start <= t_tilde <= t_hat <= t_end.
    # Case B: t_tilde <= t_start <= t_end <= t_hat.
    # Case C: t_tilde <= t_hat <= t1 = t_start = t_end.
    if t <= min(times["t_start"], times["t_tilde"]):
        # No congestion, early departures.
        to_be_exped = (
            params["beta"] * (params["tt0"] - params["tstar"] + params["delta"]) / params["mu"]
        )
        if to_be_exped < -700.0:
            # Underflow: n is practically 0.
            return 0.0
        a = (params["n"] / denominator) * np.exp(to_be_exped)
        n = (
            (params["mu"] / params["beta"])
            * a
            * (
                np.exp(params["beta"] * t / params["mu"])
                - np.exp(params["beta"] * params["period"][0] / params["mu"])
            )
        )
        return max(n, 0.0)
    elif t <= times["t_tilde"]:
        # Congestion, early departures.
        n1 = integral(times["t_start"], times, denominator, params)
        r0 = dep_rate(times["t_start"], denominator, times, params)
        if r0 <= 0.0:
            # Underflow.
            n2 = 0.0
        else:
            n2 = (
                (params["mu"] * params["bottleneck_flow"])
                / (params["alpha"] - params["beta"])
                * (
                    params["alpha"] / params["mu"] * (t - times["t_start"])
                    - np.log(
                        dep_rate(t, denominator, times, params)
                        / dep_rate(times["t_start"], denominator, times, params)
                    )
                )
            )
        return max(n1 + n2, 0.0)
    elif t <= times["t_start"] and t <= t_hat:
        # No congestion, on-time departures.
        n1 = integral(times["t_tilde"], times, denominator, params)
        n2 = (params["n"] / denominator) * (t - times["t_tilde"])
        return max(n1 + n2, 0.0)
    elif t > times["t_start"] and t <= t_hat and t <= times["t_end"]:
        # Congestion, on-time departures.
        t_prev = max(times["t_start"], times["t_tilde"])
        n1 = integral(t_prev, times, denominator, params)
        n2 = (
            (params["mu"] * params["bottleneck_flow"])
            / params["alpha"]
            * (
                params["alpha"] / params["mu"] * (t - t_prev)
                - np.log(
                    dep_rate(t, denominator, times, params)
                    / dep_rate(t_prev, denominator, times, params)
                )
            )
        )
        return max(n1 + n2, 0.0)
    elif t > times["t_start"] and t <= times["t_end"]:
        # Congestion, late departures.
        n1 = integral(t_hat, times, denominator, params)
        n2 = (
            (params["mu"] * params["bottleneck_flow"])
            / (params["alpha"] + params["gamma"])
            * (
                params["alpha"] / params["mu"] * (t - t_hat)
                - np.log(
                    dep_rate(t, denominator, times, params)
                    / dep_rate(t_hat, denominator, times, params)
                )
            )
        )
        return max(n1 + n2, 0.0)
    elif t > times["t_end"] and t <= t_hat:
        # No congestion, on-time departures.
        n1 = integral(times["t_end"], times, denominator, params)
        n2 = (params["n"] / denominator) * (t - times["t_end"])
        return max(n1 + n2, 0.0)
    else:
        # No congestion, late departures.
        if t <= times["t_start"]:
            # Congestion never starts.
            t_prev = t_hat
        else:
            t_prev = max(times["t_end"], t_hat)
        n1 = integral(t_prev, times, denominator, params)
        a0 = (params["n"] / denominator) * np.exp(
            params["gamma"] * (params["tstar"] + params["delta"] - t - params["tt0"]) / params["mu"]
        )
        a1 = (params["n"] / denominator) * np.exp(
            params["gamma"]
            * (params["tstar"] + params["delta"] - t_prev - params["tt0"])
            / params["mu"]
        )
        n2 = -(params["mu"] / params["gamma"]) * (a0 - a1)
        return max(n1 + n2, 0.0)


def equilibrium(params):
    """Returns the time thresholds and the denominator `E` such that the integral of the
    analytical departure rate over the period is equal to the number of individuals.
    """

    def diff_integral(d):
        times = equilibrium_conditional(d, params)
        n = integral(params["period"][1], times, d, params)
        return n - params["n"]

    min_v = -params["alpha"] * (params["tt0"] + params["n"] / params["bottleneck_flow"]) - max(
        params["beta"] * (params["tstar"] - params["delta"] - params["period"][0] - params["tt0"]),
        params["gamma"]
        * (
            params["period"][0]
            + params["tt0"]
            + params["n"] / params["bottleneck_flow"]
            - params["tstar"]
            - params["delta"]
        ),
    )
    min_denominator = np.exp(max(min_v / params["mu"], -700)) * (
        params["period"][1] - params["period"][0]
    )
    max_v = -params["alpha"] * params["tt0"]
    max_denominator = np.exp(max_v / params["mu"]) * (params["period"][1] - params["period"][0])
    denominator = optimize.bisect(
        diff_integral, max(1e-16, min_denominator - 1), max_denominator + 1
    )
    times = equilibrium_conditional(denominator, params)
    return times, denominator
