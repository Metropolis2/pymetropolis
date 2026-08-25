# Bottleneck, with custom steps

This example showcases how to include your own custom steps within the pipeline.
It reproduces the [Bottleneck case
study](https://docs.metropolis2.org/pymetropolis/case_study/bottleneck.html) example, with a few
extra features (systematic sampling, comparison to analytical solution).

The custom Steps are defined in a single file, [`custom_steps.py`](custom_steps.py), and registered
via:

```toml
custom_steps = ["custom_steps.py"]
```

Run the example the usual way:

```bash
pymetropolis examples/extra/bottleneck-custom-steps/config.toml
```

There are three Steps in `custom_steps.py` each demonstrating a different extension pattern.

## 1. A brand-new Step producing official files from nothing — `BottleneckNetworkStep`

In the Bottleneck case study, the road network is built as a special case of a grid network.
The new `BottleneckNetworkStep` replaces `GridNetworkStep`, `PostprocessRoadNetworkStep`, and
`ExogenousCapacitiesStep`, while simplifying the parameters that need to be defined in the config
for this simple network.
This step demonstrates how you could replace existing steps (like `EqasimImportStep` or
`OSMRoadNetworkImporstStep`) with your own tailor-made processing.
Because the step produces files that downstream Steps already know how to consume, everything past
network cleaning (primary-edge extraction, free-flow travel times, simulation) works unmodified.

It also illustrates:
- **Defining new `Parameter`s** (`bottleneck_network.free_flow_travel_time`,
  `bottleneck_network.capacity`) to expose its own config section.
- **`is_defined()`** to opt out of the pipeline entirely (so the Step are simply absent) when that
  config section isn't set, rather than being an always-on Step with `None` parameters.

## 2. Overriding a built-in Step to add a feature — `UniformDrawsStep`

`custom_steps.UniformDrawsStep` **inherits from** the built-in
`pymetropolis.metro_demand.population.draws.UniformDrawsStep` and, because it shares that class's
name, replaces it in the pipeline. It adds one new `Parameter`, `uniform_draws.systematic_sampling`,
to switch the epsilon draws used for inverse-transform sampling from independent uniform draws to
systematic sampling (which reduces sampling variance and makes the simulation converge closer to the
analytical solution below, for a given population size).

Two points worth noting:
- **`input_files`/`output_files` are not redeclared** — they're inherited as-is from the parent
  Step, and are still reachable as `self.input["trips"]` / `self.output["uniform_draws"]`.
- **`super().run()`** is called to fall back to the original implementation when
  `systematic_sampling` is disabled, so the override only has to implement the new behavior, not
  duplicate the old one.

## 3. A new output computed from existing files and other Steps' parameters — `DepartureRateComparisonStep`

`DepartureRateComparisonStep` reads the simulation's `TripResultsFile` — an existing, official
output — and compares the simulated departure rate against the analytical equilibrium departure rate
of the stochastic bottleneck model (de Palma, Ben-Akiva, Lefevre and Litinas, 1983), computed by the
external module [`analytical_solution.py`](analytical_solution.py).

Notable patterns:
- **A new `MetroFile`**, `DepartureRateComparisonPlotFile`, defined right next to the Step that
  produces it, combining `MetroPlotFile` (matplotlib-figure serialization) with `PopulationFile`
  (a `{population}`-templated path) so each population gets its own graph under
  `results/graphs/{population}/trips/departure_rate_comparison.png`.
- **Saving a graph**: `run()` builds a `matplotlib` figure and hands it to
  `self.output["plot"].write(fig)`.
- **Reading other Steps' parameters**: the analytical formula needs values that are configured
  and normally read elsewhere (`simulation.period`, `departure_time_choice.mu`,
  `modes.car_driver.alpha`, the `departure_time.linear_schedule.*` keys, plus this file's own
  `bottleneck_network.*` keys). None of these are "owned" by this Step, but any config key can be
  exposed to a Step by declaring a `Parameter` for it.
