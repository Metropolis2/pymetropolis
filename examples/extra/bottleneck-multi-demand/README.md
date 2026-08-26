# Bottleneck, with two demand populations

This example showcases how to run a simulation with several demand populations. It reuses the
[Bottleneck case
study](https://docs.metropolis2.org/pymetropolis/case_study/bottleneck.html) network, split into
two independent populations of car drivers with different desired arrival times.

Run the example the usual way:

```bash
pymetropolis examples/extra/bottleneck-multi-demand/main-config.toml
```

## Config layout

The infrastructure shared by every population — the grid network, the road network and the
simulation settings — is defined once in [`main-config.toml`](main-config.toml). Demand-related
sections (`node_od_matrix`, `mode_choice`, `modes`, `departure_time_choice`,
`departure_time.linear_schedule`) are defined separately in [`pop1.toml`](pop1.toml) and
[`pop2.toml`](pop2.toml), each declaring its own `population_name` and, for this example, an
identical OD matrix and mode/departure-time-choice setup except for `departure_time.linear_schedule.tstar`
(7:45 for `pop1`, 7:15 for `pop2`), so the two populations converge to different departure-time
peaks around the shared bottleneck.

The two population files are registered from the main config with:

```toml
extra_populations = ["pop1.toml", "pop2.toml"]
```

Note that `main_population = false` is also set: the main config only holds the shared
infrastructure sections and defines no population of its own — `pop1` and `pop2` are the only two
populations simulated, there is no implicit third "main" population alongside them. Without
`main_population = false`, any demand-related config key set directly in `main-config.toml` (rather
than in one of the population files) would define an extra, third population there.

## What to look at

- Every population-specific file, both intermediate and final, is written under a
  `{population}`-namespaced path, e.g. `output/demand/pop1/...` and `output/demand/pop2/...` for
  inputs, `output/results/pop1/trip_results.parquet` and `output/results/pop2/trip_results.parquet`
  for results, so the two populations' files never collide.
- Steps that need the demand of every population at once (e.g., to build the simulation-wide agents
  file consumed by Metropolis-Core) merge `pop1` and `pop2`'s rows together, prefixing id columns
  with the population name so they stay globally unique.
- Outputs that describe the shared network or the simulation as a whole rather than any one
  population (e.g., the convergence graphs and `output/results/iteration_results.parquet`) report
  both populations combined.
