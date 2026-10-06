# Pymetropolis

## Overview

Pymetropolis is a Python pipeline library that drives METROPOLIS2, an
agent-based transport simulator. Users write a TOML configuration file
describing a pipeline and run it with:

```bash
pymetropolis myconfig.toml
```

The pipeline executes a sequence of **Steps**. Each Step reads inputs,
does some work (often calling out to METROPOLIS2 or processing its
output), and writes outputs. On re-execution, Pymetropolis diffs the
config against the previous run and only re-runs Steps whose inputs or
parameters actually changed — comparable to a build system like Make
or Snakemake. Keep this caching/invalidation model in mind whenever
touching Step definitions: a Step's declared inputs/config keys must
accurately reflect what it actually depends on, or the cache will be
wrong (stale results reused, or unnecessary re-runs).

Every config has a mandatory `main_directory` option: the root where
all files generated during a pipeline run are stored. They are called
MetroFiles and are mostly Parquet or GeoParquet files.

## Pipeline syntax

The core abstractions live in `src/pymetropolis/metro_pipeline/`:
`Step` (steps.py), `MetroFile` (file.py), and `Parameter` (parameters.py).
Read those base classes before adding or modifying a Step.

### MetroFile

A `MetroFile` subclass declares one file that can be produced/consumed
under `main_directory`. Concrete base classes handle serialization:
`MetroDataFrameFile` (Parquet via polars), `MetroGeoDataFrameFile`
(GeoParquet via geopandas), `MetroTxtFile`, `MetroPlotFile`,
`MetroMLEstimatorFile`. `MetroDataFrameFile`/`MetroGeoDataFrameFile`
validate columns against a `schema` on write.

```python
from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType


class TripsDistancesFile(MetroDataFrameFile):
    path = "demand/population/trips/distances.parquet"
    description = "Euclidean distance of each trip."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "od_distance",
            MetroDataType.FLOAT,
            description="Distance between origin and destination, in meters.",
            nullable=False,
        ),
    ]
```

- `path` is relative to `main_directory`.
- `Column(name, dtype, optional=False, nullable=True, unique=False, description=None)`
  — `optional` means the column may be absent from the DataFrame;
  `nullable`/`unique` are enforced when the column is present.
  Columns not listed in `schema` are dropped on write (with a warning)
  unless `discard_extra_columns = False`.

### Parameter

A `Parameter` binds a config TOML key (dotted path, e.g.
`"synthetic_population.fraction"`) to a typed, validated attribute on
a Step. Typed subclasses exist for common cases: `BoolParameter`,
`IntParameter`/`FloatParameter` (with `lower_bound`/`upper_bound`),
`FractionParameter` (float in `[0, 1]`), `StringParameter`,
`DateParameter`, `TimeParameter`, `DurationParameter`, `EnumParameter`
(`values=[...]`), `PathParameter` (`check_file_exists`,
`check_dir_exists`, `extensions`), `ExecPathParameter`, `ListParameter`
(`inner=`, `length`/`min_length`/`max_length`), and `CustomParameter`
(arbitrary `validator` callable). `random.py` and `metro_spatial/crs.py`
add distribution-valued and CRS-valued parameters (`FloatDistributionParameter`,
`GeoStep.crs`, etc.) as further examples.

```python
from pymetropolis.metro_pipeline.parameters import FractionParameter, PathParameter


class EqasimImportStep(Step):
    eqasim_output = PathParameter(
        "synthetic_population.eqasim_output",
        check_dir_exists=True,
        description="Path to the output directory of the Eqasim synthetic population pipeline.",
    )
    fraction = FractionParameter(
        "synthetic_population.fraction",
        default=1.0,
        description="Fraction of the synthetic population to be selected for simulations.",
    )
```

Declared parameters become instance attributes (`self.fraction`) once
the Step is constructed with a `Config`. A parameter with no `default`
resolves to `None` if the TOML key is absent.

Pass `shared=True` for a Parameter that is genuinely global rather than
per-population (e.g. `GeoStep.crs`, `RandomStep.random_seed`): for an
extra population, a `shared` Parameter falls back to the main config
when the key isn't set in that population's own config file; a
non-`shared` Parameter (the default) resolves to `None` in that case
even if the main config happens to define the same key — see
"Populations" below.

### Step

A `Step` subclass declares:

- `input_files: dict[str, type[MetroFile] | InputFile]` — files read by
  `run()`. Wrap in `InputFile(FileClass, optional=True, when=lambda step: ...)`
  for optional/conditional inputs; a bare `MetroFile` class means
  required and always-needed. `InputFile(FileClass, all_populations=True)`
  reads a `PopulationFile` for every configured population at once —
  see "Populations" below.
- `output_files: dict[str, type[MetroFile]]` — files written by `run()`.
- Any number of `Parameter` class attributes — these are the config
  keys the Step depends on for cache invalidation, so declare every
  config value `run()` actually reads.
- `run(self)` — the step's logic. Read inputs with
  `self.input["name"].read()`, write outputs with
  `self.output["name"].write(value)`.
- Optionally `priority: ClassVar[int]` (default `1`; `0` marks a
  non-primary step that only runs if another primary step needs its
  output) and `is_defined(self) -> bool` (return `False` when required
  parameters are missing, so the step is skipped rather than run).

```python
from pymetropolis.metro_demand.population.files import (
    TripsDestinationsFile,
    TripsDistancesFile,
    TripsOriginsFile,
)
from pymetropolis.metro_pipeline import Step


class TripDistancesStep(Step):
    """Computes the Euclidean distances between origin and destination for each trip."""

    input_files = {"origins": TripsOriginsFile, "destinations": TripsDestinationsFile}
    output_files = {"distances": TripsDistancesFile}

    def run(self):
        import polars as pl

        origins = self.input["origins"].read()
        destinations = self.input["destinations"].read()
        distances = pl.DataFrame(
            {"trip_id": origins["trip_id"], "od_distance": origins.distance(destinations)}
        )
        self.output["distances"].write(distances)
```

Mix in `RandomStep` (adds `self.random_seed` / `self.get_rng(str(self))`,
from `pymetropolis/random.py`) or `GeoStep` (adds `self.crs`, from
`metro_spatial/crs.py`) when a Step needs randomness or a projected
CRS — see `EqasimImportStep` in
`src/pymetropolis/metro_demand/population/eqasim.py` for a Step
combining both, an optional `InputFile`, and a multi-file output.
Always call `get_rng` with the calling step's own `str(self)` (never a
literal or another step's), so that steps/populations sharing
`random_seed` still draw independent random sequences.

New Steps must be added to the relevant package's step list (see how
`POPULATION_STEPS` in `src/pymetropolis/metro_demand/population/__init__.py`
aggregates the Steps for that package) so `MetroPipeline` can discover
them. New MetroFiles go in the matching `*_FILES` list the same way.
Each `metro_*` package's `__init__.py` aggregates these into
`STEPS`/`FILES`, and `src/pymetropolis/schema.py` concatenates all
packages into the global `STEPS` (passed to `MetroPipeline` by
`cli.py`) and `FILES` (used by `bin/generate_doc.py` for the reference
docs).

Top-level packages: `metro_spatial` (CRS, simulation area, urban
areas), `metro_network` (road/PT/bicycle/pedestrian networks, OSM
import), `metro_demand` (zones, OD matrices, synthetic populations,
modes, departure times, routing), `metro_simulation` (writing
METROPOLIS2 inputs and running it), `metro_results` (post-processing
and plots), `metro_calibration`, `metro_environment`; shared helpers
are in `metro_common`.

Users can also add or override Steps from their own Python files via
the `custom_steps` config key (`MetroPipeline.load_custom_steps`: a
custom Step with the same class name as an existing one replaces it) —
see `examples/extra/bottleneck-custom-steps/`.

### Populations

A run can simulate several independent demand populations (e.g. a
synthetic population of persons plus a truck OD matrix) sharing common
infrastructure (road network, simulation settings). The main config
always defines one implicit "main" population; additional ones are
declared with `extra_populations = ["persons.toml", "trucks.toml"]`
(paths relative to the main config file), where each listed file is a
normal, standalone TOML config with its own `population_name = "..."`
key plus whatever population-specific sections it needs (using the
exact same dotted keys as the main config, e.g. `[synthetic_population]`
— no renaming). Set `main_population = false` in the main config if it
should not itself define a population (only the extra ones are used).
Population names must be unique, may not contain `"-"`, and (when
`main_population` is true) may not reuse the reserved main-population
name (`"population"`) — `Config.read_extra_populations` rejects all of
these at config-load time.

- `PopulationFile` (a `MetroFile` mixin, `file.py`) is for files whose
  location depends on which population produced them. Its `path` must
  contain a `{population}` placeholder, e.g.
  `path = "demand/{population}/trips.parquet"`.
- `PopulationStep` (a `Step` subclass, `steps.py`) is for Steps that run
  once per population. `Config.instantiate_step` builds one instance
  for the main population (`step_class(config)`, `population_name`
  defaults to the `"population"` sentinel) plus one per extra
  population (`step_class.for_population(config, name)`); each
  instance resolves its `Parameter`s against that population's own
  config (falling back to the main config only for `shared`
  Parameters — see "Parameter" above) and its `PopulationFile`
  inputs/outputs against that population's namespaced path. `str(step)`
  is `"<name>__<ClassName>"` for an extra population (plain
  `<ClassName>` for the main one), which is also the JSON cache
  filename, so populations never collide on cache or output paths.
- A plain (non-`PopulationStep`) Step that needs to consume every
  population's copy of a `PopulationFile` at once (typically to merge
  them into a single file for the actual METROPOLIS2 input — see
  `WriteMetroAgentsStep` in
  `src/pymetropolis/metro_simulation/demand/agents.py`) declares that
  input as `InputFile(FileClass, all_populations=True)` and reads it
  via `self.input_populations["name"]` (a `dict[population_name,
  MetroFile]`), not `self.input["name"]`. By default (`optional=False`)
  the step is only feasible once *every* configured population has
  produced the file; pass `optional=True` if the step should run
  regardless of how many populations actually produce it (there is no
  built-in "at least one, but not necessarily all" option). When
  merging rows from several populations into one file, prefix id
  columns with the population name (see `merge_populations` in
  `metro_simulation/common.py`) so ids stay globally unique — this
  relies on population names never containing `"-"` (the prefix
  delimiter), which `Config.read_extra_populations` rejects.
- Never override `__init__` on a `PopulationStep` subclass —
  `for_population` builds extra-population instances via `cls.__new__`
  plus `_init_from_config`, bypassing `__init__` entirely (enforced by
  a check in `PopulationStep.__init_subclass__`).

### Config inheritance (`parent_config`)

A config can inherit its values from another config:

```toml
main_directory = "scenario2/"
parent_config = "scenario1.toml"

[modes.car_driver]
alpha = 20
```

`parent_config` (resolved relative to the config file, `Config.read_parent_config`) is loaded as
its own full, standalone `Config`, not merged into this one's dict — so the parent always resolves
its own relative paths and `"secret:"`/`"env:"` indirections against itself, whether it is run
directly or only inherited from. A key not set by the config itself is looked up in its parent,
then that parent's parent, and so on (`Config.chain()`); population names, `custom_steps`, and
`main_population` are inherited the same way. This is meant for a family of closely related
scenarios that only need to override a handful of keys relative to a base config — see
`examples/extra/bottleneck-multi-scenario/`.

When `parent_config` is set, `MetroPipeline` additionally decides, per Step, whether that Step's
output canonically belongs to an ancestor config rather than to the config actually being run
(`reuse.compute_source_config`): this holds when an equivalent Step (same class, same population)
exists in that ancestor with an identical `config_hash()` (every `Parameter` it reads resolves to
the same value there), *and* every input file it reads is, in that ancestor's own graph, produced
by a Step with the same resolved source — so a Step whose own parameters are unchanged can still
fail to be reused if one of its upstream inputs was changed by an intermediate config. Steps found
this way are then rebased (`graph.rebase_step_graph`) to actually read/write under that ancestor's
`main_directory` instead of the derived config's own — including when the ancestor has never
itself been run, in which case running the derived config computes the Step once and writes it
into the ancestor's directory, so the ancestor (or any other config derived from it with the same
parameters) then finds it already fresh. `--dry-run` annotates a reused Step with
`(from: <config file name>)`. See the module docstring in
`src/pymetropolis/metro_pipeline/reuse.py` for the full decision logic, including its known
limitation around `custom_steps`-overridden Step classes.

### How config maps to a run

`MetroPipeline` takes a `Config` (parsed from the TOML file) and the
full list of known Step classes, and instantiates each one via
`Config.instantiate_step` — one instance for a plain `Step`, or (for a
`PopulationStep`) one per configured population, per "Populations"
above — keeping only those that are `is_defined()` and have declared
outputs. It then topologically orders Steps by matching `output_files`
to `input_files` (comparing resolved `MetroFile` instances, not
classes, so different populations' namespaced files never conflict
with each other), resolves conflicts (multiple Steps producing the
same file) by `priority`/output-count/class-name, and — per Step —
compares current parameter values and input/output file mtimes
against the JSON cache under `main_directory/update_files/<str(step)>.json`
to decide whether to skip, run (`OUTDATED`), or re-run because an
upstream input changed (`INVALIDATED`).

## Environment & tooling

- **Package management**: [uv](https://docs.astral.sh/uv/). Use `uv run <cmd>`
  rather than invoking tools from a manually-activated venv. Use
  `uv add <package>` / `uv remove <package>` to change dependencies —
  don't hand-edit `pyproject.toml` dependency lists.
- **Linting/type-checking**: [ty](https://github.com/astral-sh/ty).
  Run with `uv run ty check`.
- **Formatting**: [ruff](https://docs.astral.sh/ruff/). Run with
  `uv run ruff format` and `uv run ruff check --fix`.
- **Testing**: [pytest](https://docs.pytest.org/), tests live under
  `tests/` in files named `*_test.py` (not `test_*.py`). Run with
  `uv run pytest`. `dot_test.py` needs Graphviz (`dot`) installed.
- **Running the pipeline manually**: `uv run pymetropolis path/to/config.toml`.
  Running METROPOLIS2 needs the `metropolis_cli` and `routing_cli`
  executables, configured through the `METROPOLIS_EXEC_PATH` /
  `METROPOLIS_ROUTING_EXEC_PATH` environment variables or the
  `metropolis_core.exec_path` / `metropolis_core.routing_exec_path` parameters.
  The CLI loads a `.env` file from the working directory, if any
  (see `.env.example`); config string values may use `"env:VAR"` /
  `"secret:key"` indirections (`Config.resolve_indirection`).
- **Examples**: `examples/case-studies/` mirror the official case
  studies at docs.metropolis2.org; `examples/extra/` show
  multi-population, multi-scenario and custom-step setups. Paths inside
  a config, including `main_directory`, are resolved relative to the
  config file.

Common commands:

```bash
# Install/sync dependencies
uv sync

# Format + lint + type-check (run before considering a change done)
uv run ruff format
uv run ruff check --fix
uv run ty check

# Run the test suite, a single file, or a single test
uv run pytest
uv run pytest tests/pipeline_test.py
uv run pytest tests/pipeline_test.py::test_basic_pipeline

# Run the pipeline against a sample config
uv run pymetropolis examples/case-studies/bottleneck/config.toml

# Show what would run without running it (reused steps show "(from: <config>)")
uv run pymetropolis path/to/config.toml --dry-run

# Run only one Step (and what it needs); save the Step graph (rendering needs Graphviz)
uv run pymetropolis path/to/config.toml --step TripDistancesStep
uv run pymetropolis path/to/config.toml --graph steps.svg

# Regenerate the Step/MetroFile reference docs as Markdown
uv run python src/pymetropolis/bin/generate_doc.py <output_dir>
```

Always run `ruff format`, `ruff check --fix`, `ty check`, and
`pytest` after making changes, and fix anything they flag before
treating a task as finished. These same checks (including `pytest`)
run in CI on every pull request and push to `main`
(`.github/workflows/lint.yml`, Python 3.14). Ruff uses a line length
of 100 with `skip-magic-trailing-comma = true`.

## Conventions

- Python versions: development and CI use the version pinned in
  `.python-version` (3.14), which is also the one recommended to users,
  but code must stay compatible with the minimum version declared in
  `pyproject.toml` (`requires-python = ">=3.12"`) — don't use syntax or
  stdlib features newer than that. Within that limit, use modern syntax
  (e.g. `X | None` over `Optional[X]`, builtin generics over
  `typing.List`/`typing.Dict`).
- Type-hint all new/modified function signatures.
- New pipeline Steps should follow the existing Step interface (see
  the base Step class) so they participate correctly in dependency
  tracking and cache invalidation. When adding a Step, be explicit
  about:
  - what config keys it reads,
  - what files/Steps it depends on,
  - what it writes under `main_directory`.
- Prefer `pathlib.Path` over raw strings for anything under
  `main_directory`.

## Notes for Claude

- Don't assume file layout beyond what's shown in context — inspect
  the repo structure before editing rather than guessing module paths.
- Run the format/lint/type-check commands above before calling a
  change complete.
