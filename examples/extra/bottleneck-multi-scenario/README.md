# Bottleneck, with parent config

This example showcases `parent_config`: a config that inherits its values from another config,
overriding only the keys it actually needs. It reuses the [Bottleneck case
study](https://docs.metropolis2.org/pymetropolis/case_study/bottleneck.html) network and demand,
run three times with different behavioral parameters.

Run the three scenarios the usual way, in order:

```bash
pymetropolis examples/extra/bottleneck-multi-scenario/scenario1.toml
pymetropolis examples/extra/bottleneck-multi-scenario/scenario2.toml
pymetropolis examples/extra/bottleneck-multi-scenario/scenario3.toml
```

## Config layout

[`scenario1.toml`](scenario1.toml) is a normal, standalone config defining the whole Bottleneck
case study: grid network, road network, OD matrix, mode choice, and the linear scheduling utility
(`beta`/`gamma`/`tstar`) with `departure_time_choice.mu = 1`.

[`scenario2.toml`](scenario2.toml) declares:

```toml
parent_config = "scenario1.toml"
```

and overrides only `modes.car_driver.alpha` (the value of time) and
`departure_time.linear_schedule.beta`/`gamma` (the schedule penalties), doubling all three. Every
other key — the network, the OD matrix, `tstar`, `departure_time_choice.mu` — is inherited
unchanged from `scenario1.toml`.

[`scenario3.toml`](scenario3.toml) declares `parent_config = "scenario2.toml"` and overrides only
`departure_time_choice.mu`, the logit scale of the departure-time choice model. Every other key is
inherited from `scenario2.toml`, and, transitively, from `scenario1.toml`.

A key is looked up in the config itself first, then in its parent, then in its parent's parent,
and so on — so `scenario3.toml`'s `[grid_network]` section (absent from both `scenario2.toml` and
`scenario3.toml`) ultimately resolves to `scenario1.toml`'s.

## What to look at

- **Steps are reused across scenarios, not recomputed.** A Step is only rerun by a derived config
  (`scenario2`, `scenario3`) if the config value(s) it reads, or one of its upstream inputs,
  actually differ from the parent it inherits from. Here, `scenario2.toml` only changes
  `modes.car_driver.alpha` and the linear-schedule penalties, so it recomputes the demand and
  simulation Steps downstream of those parameters, but reuses `scenario1`'s network and
  synthetic-population Steps unchanged (e.g. `GridNetworkStep`). `scenario3.toml` only changes
  `departure_time_choice.mu`, so it reuses everything upstream of the departure-time choice model
  — including `LinearScheduleStep`, whose parameters it inherits from `scenario2`, not
  `scenario1` — and only recomputes the departure-time choice and simulation Steps themselves.
- **A reused Step's output lives in the config that owns it, not in the derived config's own
  `main_directory`.** Run `pymetropolis --dry-run scenario3.toml` to see this: every reused Step is
  annotated with `(from: scenario1.toml)` or `(from: scenario2.toml)`, and running `scenario3.toml`
  for real never writes those Steps' outputs under `scenario3/` — it reads and, if needed, computes
  them directly under `scenario1/` or `scenario2/`. `scenario3.toml` does not need `scenario1.toml`
  or `scenario2.toml` to have been run beforehand: `parent_config` always reads the parent config
  file itself, so running `scenario3.toml` first computes whichever ancestor Steps it needs,
  writing each one under its owning scenario's own `main_directory` — the three scenarios can be
  run in any order, or even just the one you actually care about.
- **Only the overridden keys need to be repeated.** Compare the size of `scenario2.toml` and
  `scenario3.toml` to `scenario1.toml`: a derived config is a diff against its parent, not a full
  copy, which keeps a family of related scenarios easy to read and to keep consistent.
