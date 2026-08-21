# Examples

This directory contains example [Pymetropolis](https://github.com/Metropolis2/pymetropolis)
configurations, meant to be run as-is or used as a starting point for your own simulations.

Every config's `main_directory` (and other paths declared in it) are resolved relative to the
config file itself, so any of them can be run from anywhere, e.g.:

```shell
pymetropolis examples/case-studies/bottleneck/config.toml
```

Running a simulation requires the `metropolis_cli` and `routing_cli` executables from
[Metropolis-Core](https://github.com/Metropolis2/Metropolis-Core/releases).
See
[Getting started](https://docs.metropolis2.org/pymetropolis/getting_started.html#configuring-the-metropolis-core-executables-)
for how to point Pymetropolis to them (environment variables, or the `metropolis_core.exec_path`
/ `routing_exec_path` parameters).

## `case-studies/`

Companion configuration files for the
[official case studies](https://docs.metropolis2.org/pymetropolis/case_study/index.html),
documented step-by-step in the Book. Kept in sync with the walkthroughs there.

- [`bottleneck/config.toml`](case-studies/bottleneck/config.toml): toy single-road,
  single-bottleneck network with stochastic departure-time choice
  ([Bottleneck case study](https://docs.metropolis2.org/pymetropolis/case_study/bottleneck.html)).
- [`circular-city/config.toml`](case-studies/circular-city/config.toml): synthetic circular city
  with mode choice, departure-time choice and route choice
  ([Circular City case study](https://docs.metropolis2.org/pymetropolis/case_study/circular_city.html)).
- [`circular-city/config-ridesharing.toml`](case-studies/circular-city/config-ridesharing.toml):
  same network, with a ridesharing mode added.
- [`chambery/config.toml`](case-studies/chambery/config.toml): real-world case study around
  Chambéry, France, importing an OSM road network and GTFS public-transit data, and generating
  demand from a synthetic population.

## `extra/`

Additional example configurations, not covered by the Book, showcasing other Pymetropolis
features.

- [`bottleneck-2pop/`](extra/bottleneck-2pop/): the Bottleneck network split into two demand
  populations with different desired arrival times, demonstrating multi-population support
  (`extra_populations`).
- [`france/config-joint-tours.toml`](extra/france/config-joint-tours.toml): trains a joint-tours
  classifier from an aggregation of French mobility surveys, imported with
  [MobiSurvStd](https://github.com/Metropolis2/mobisurvstd).
