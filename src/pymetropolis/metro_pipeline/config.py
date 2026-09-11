from __future__ import annotations

import os
import tomllib
from pathlib import Path
from tomllib import TOMLDecodeError
from typing import TYPE_CHECKING, Any

from loguru import logger

from pymetropolis.metro_common import MetropyError

from .steps import MAIN_POPULATION_NAME, PopulationStep, Step

if TYPE_CHECKING:
    from collections.abc import Iterator

# Top-level configuration key used to specify the main directory location.
MAIN_DIR_KEY = "main_directory"
# Top-level configuration key used to specify the secrets file location.
SECRETS_KEY = "secrets_file"
# Top-level configuration key used to specify the population-specific configs.
POPULATIONS_KEY = "extra_populations"
# Top-level configuration key used to specify the population name.
POP_NAME_KEY = "population_name"
# Top-level configuration key used to specify whether the main / default population (defined
# directly in the main config) should be used.
MAIN_POPULATION_KEY = "main_population"
# Top-level configuration key used to specify Python files defining custom Step classes.
CUSTOM_STEPS_KEY = "custom_steps"
# Top-level configuration key used to specify a config file whose values are inherited.
BASE_CONFIG_KEY = "base_config"


def parse_toml(path: Path) -> dict:
    if not path.is_file():
        raise MetropyError(f"File does not exist: {path.absolute()}")
    with open(path, "rb") as f:
        try:
            d = tomllib.load(f)
        except TOMLDecodeError as e:
            logger.error(f"Failed to parse TOML file: `{path}`")
            raise e
    return d


class Config:
    main_directory: Path
    main_path: Path | None
    dict: dict[str, Any]
    # The extra populations declared by *this* config only, never the inherited ones: a value read
    # from here is attributed to `self`, so holding a base config's populations would resolve their
    # relative paths against the wrong directory. Use `population_names` for the effective list.
    extra_populations_dict: dict[str, dict]
    secrets: dict[str, Any]
    main_population: bool
    custom_step_paths: list[Path]
    # The config this one inherits its values from, if any (see `read_base_config`).
    base_config: Config | None
    # Effective names of the extra populations, across the whole chain of base configs.
    population_names: list[str]

    def __init__(
        self, d: dict, main_path: Path | None = None, _base_chain: list[Path] | None = None
    ):
        self.dict = d
        # Normalized, so that the values of a config resolve to the exact same paths however that
        # config was reached: directly, or as the base config of another one (whose own directory
        # would otherwise leak into the resolved paths, e.g. `derived/../base/data.csv`). Step
        # parameters store *resolved* paths and hash them, so two spellings of the same path would
        # be two different `config_hash` values, and a derived run would re-run every step reading
        # a path from its base config.
        self.main_path = main_path.resolve() if main_path is not None else None
        # Must come first: every other step below may consult the base config.
        self.read_base_config(_base_chain)
        self.check_main_directory()
        self.read_secrets()
        self.read_main_population()
        self.read_extra_populations()
        self.read_custom_steps()

    @classmethod
    def from_toml(cls, path: Path):
        """Initializes a Config from the path to a TOML file.

        Raises an exception if the given filename does not exist or is an invalid TOML file.
        """
        input_dict = parse_toml(path)
        inst = cls(input_dict, path)
        return inst

    def read_base_config(self, base_chain: list[Path] | None = None):
        """Reads the config file this config inherits its values from, if one is declared.

        The base config is loaded as a full, standalone `Config` rather than merged into
        `self.dict`, so that each config of the chain resolves *its own* values: relative paths
        against its own directory, `"secret:"` indirections against its own secrets file. This is
        what lets a derived run reuse the base run's cached steps: a path written in the base
        config resolves to the same absolute path as it did in the base run, so the step's
        `config_hash` is unchanged.

        A relative path is resolved against the directory of this config file.

        `base_chain` holds the resolved paths of the configs currently being loaded, so that a
        cycle (`a.toml` -> `b.toml` -> `a.toml`) is reported instead of recursing forever.
        """
        self.base_config = None
        base_def = self.dict.get(BASE_CONFIG_KEY)
        if base_def is None:
            return
        if not isinstance(base_def, str):
            raise MetropyError(
                f"Config value `{BASE_CONFIG_KEY}` should be a path, got `{base_def}`"
            )
        # `self.main_path` is already normalized; `path` is not (it may contain "..").
        path = self.resolve_path(base_def).resolve()
        chain = base_chain or []
        if self.main_path is not None:
            chain.append(self.main_path)
        if path in chain:
            chain_str = " -> ".join(str(p) for p in (*chain, path))
            raise MetropyError(f"Cyclic `{BASE_CONFIG_KEY}` chain: {chain_str}")
        # Note. Building the base Config runs its own `check_main_directory`, which creates its
        # `main_directory` (and `update_files/`) if needed. This is idempotent and harmless.
        self.base_config = Config(parse_toml(path), path, _base_chain=chain)

    def chain(self) -> Iterator[Config]:
        """Yields `self`, then its base config, then that config's base config, and so on.

        Configs are yielded nearest first, i.e. in decreasing order of priority.
        """
        config: Config | None = self
        while config is not None:
            yield config
            config = config.base_config

    @property
    def base_directories(self) -> list[Path]:
        """The `main_directory` of each base config of the chain, nearest first.

        Empty when this config does not inherit from any other config.
        """
        return [config.main_directory for config in self.chain() if config is not self]

    def check_main_directory(self):
        """Asserts that `main_directory` is properly defined and that the directory exists.

        If the directory does not exist, creates it.

        A relative `main_directory` is resolved against the directory of the main config file.

        Unlike every other value, `main_directory` is *not* inherited from a base config: it must
        be declared by each config and must differ from the `main_directory` of every config it
        inherits from, otherwise a derived run would overwrite the base run's output files and
        caches.
        """
        main_dir = self.dict.get(MAIN_DIR_KEY)
        if main_dir is None:
            raise MetropyError(f"Missing `{MAIN_DIR_KEY}` in config")
        if not isinstance(main_dir, str):
            raise MetropyError(f"Config value `{MAIN_DIR_KEY}` should be a path, got `{main_dir}`")
        path = self.resolve_path(main_dir)
        # Checked before creating the directory, so that an invalid config creates nothing.
        # Paths are resolved for the comparison so that two different ways of spelling the same
        # directory are caught; `self.main_directory` itself is kept as-is.
        for base_config in self.chain():
            if base_config is self:
                continue
            if path.resolve() == base_config.main_directory.resolve():
                raise MetropyError(
                    f"`{MAIN_DIR_KEY}` (`{path}`) is identical to the `{MAIN_DIR_KEY}` of the "
                    f"base config `{base_config.main_path}`: a config inheriting from another "
                    "config must write to its own directory, otherwise it would overwrite the "
                    "base run's output files and caches"
                )
        path.mkdir(exist_ok=True, parents=True)
        self.main_directory = path
        # Also create the update_files/ directory if needed.
        update_files_path = path / "update_files"
        update_files_path.mkdir(exist_ok=True)

    def read_secrets(self):
        """Reads the secrets file if it exists.

        If the SECRETS_KEY config key is not defined, the default path is `secrets.toml`.

        A relative path is resolved against the directory of the main config file.
        """
        secrets_file_def = self.dict.get(SECRETS_KEY)
        if secrets_file_def is not None and not isinstance(secrets_file_def, str):
            raise MetropyError(
                f"Invalid `{SECRETS_KEY}` parameter: Not a path: `{secrets_file_def}`"
            )
        # When not specified, default path is `secrets.toml`.
        path = self.resolve_path(secrets_file_def or "secrets.toml")
        if secrets_file_def is not None and not path.exists():
            raise MetropyError(f"Invalid `{SECRETS_KEY}` parameter: Path `{path}` does not exist")
        if path.exists():
            self.secrets = parse_toml(path)
        else:
            # Do not raise an error when the default file path does not exist.
            logger.debug(f"Secrets file path does not exist: `{path}`")
            self.secrets = dict()

    def read_extra_populations(self):
        """Reads the configuration files for the extra populations, if they are defined.

        Paths are resolved relative to the main config file.

        The populations declared by a base config are inherited. A population re-declared by this
        config (i.e. with the same `population_name` as one of the base config's) *overrides* the
        base config's declaration, key by key, instead of being rejected as a duplicate: the
        duplicate-name check below is therefore local to this config.
        """
        populations = self.dict.get(POPULATIONS_KEY, [])
        if not isinstance(populations, list):
            raise MetropyError(
                f"Invalid `{POPULATIONS_KEY}` parameter: list of path expected, got `{populations}`"
            )
        self.extra_populations_dict = dict()
        used_names = set()
        if self.main_population:
            used_names.add(MAIN_POPULATION_NAME)
        for pop_config in populations:
            if not isinstance(pop_config, str):
                raise MetropyError(f"Invalid population config file: Not a path: `{pop_config}`")
            path = self.resolve_path(pop_config)
            pop_dict = parse_toml(path)
            name = pop_dict.get(POP_NAME_KEY)
            if name is None:
                raise MetropyError(
                    f"No `{POP_NAME_KEY}` parameter in population config file `{path}`"
                )
            if not isinstance(name, str):
                raise MetropyError(
                    f"`{POP_NAME_KEY}` parameter is not a string in population config file `{path}`"
                )
            if name in used_names:
                raise MetropyError(f"Duplicate population name: `{name}`")
            if "-" in name:
                raise MetropyError(f'Population names cannot contain the "-" character ({name})')
            used_names.add(name)
            self.extra_populations_dict[name] = pop_dict
        # Effective population names, across the whole chain of base configs. The base config's
        # populations come first, in its own order, so that declaring an extra population in a
        # derived config appends it instead of reordering the base run's populations (step
        # instantiation order and `merge_populations` id prefixes then stay stable).
        names = self.base_config.population_names if self.base_config is not None else []
        for name in self.extra_populations_dict:
            if name not in names:
                names.append(name)
        self.population_names = names

    def read_main_population(self):
        """Reads whether the main / default population (defined directly in the main config)
        should be used. Defaults to the base config's value, or to `True`.
        """
        default = self.base_config.main_population if self.base_config is not None else True
        value = self.dict.get(MAIN_POPULATION_KEY, default)
        if not isinstance(value, bool):
            raise MetropyError(f"`{MAIN_POPULATION_KEY}` parameter should be a boolean: `{value}`")
        self.main_population = value

    def read_custom_steps(self):
        """Reads the paths to the Python files defining custom Step classes, if any are defined.

        Paths are resolved relative to the main config file.

        The files declared by a base config are kept, *before* this config's own files:
        `MetroPipeline.load_custom_steps` lets a Step defined in a later file override a Step of
        the same name defined in an earlier one, so a derived config can redefine one of the base
        config's custom Steps.
        """
        custom_steps = self.dict.get(CUSTOM_STEPS_KEY, [])
        if not isinstance(custom_steps, list):
            raise MetropyError(
                f"Invalid `{CUSTOM_STEPS_KEY}` parameter: list of path expected, got "
                f"`{custom_steps}`"
            )
        # The base config's paths were already validated when it was read.
        paths = self.base_config.custom_step_paths if self.base_config is not None else []
        for custom_step in custom_steps:
            if not isinstance(custom_step, str):
                raise MetropyError(f"Invalid custom step file: Not a path: `{custom_step}`")
            path = self.resolve_path(custom_step)
            if not path.is_file():
                raise MetropyError(f"Custom step file does not exist: `{path}`")
            paths.append(path)
        # Drop duplicates (a derived config may restate one of the base config's files), keeping
        # the last occurrence so that the order declared by this config wins. Without this, the
        # same file would be loaded twice, under two different synthetic module names, and would
        # be reported as overriding itself.
        deduplicated: dict[Path, Path] = dict()
        for path in paths:
            deduplicated.pop(path.resolve(), None)
            deduplicated[path.resolve()] = path
        self.custom_step_paths = list(deduplicated.values())

    def instantiate_step(self, step_class: type[Step]) -> list[Step]:
        steps = list()
        if not issubclass(step_class, PopulationStep) or self.main_population:
            steps.append(step_class(self))
        if issubclass(step_class, PopulationStep):
            for pop_name in self.population_names:
                steps.append(step_class.for_population(self, pop_name))
        return steps

    def _lookup_parameter(
        self, key: list[str], population_name: str | None = None, shared: bool = False
    ) -> tuple[Any, Config] | None:
        """Finds the config of the chain defining `key`; returns the raw value and that config.

        Values are inherited along the chain of base configs, so the lookup is done in two passes,
        the population config files first and the main config files second. This way, a
        population-specific value always takes precedence over a main-config value, whichever
        config of the chain each of them comes from: the selected value is the one that a deep
        merge of the whole chain would give.

        1. If `population_name` is given, that population's own config file, in each config of the
           chain. A non-`shared` parameter stops there and is never read from a main config.
        2. The main config file of each config of the chain.

        Returns `None` if the key is not defined by any config of the chain.
        """
        if population_name is not None:
            if population_name not in self.population_names:
                raise MetropyError(f"Unknown population: `{population_name}`")
            for config in self.chain():
                pop_dict = config.extra_populations_dict.get(population_name)
                if pop_dict is None:
                    # That config of the chain does not declare this population.
                    continue
                value = self._resolve_from_dict(pop_dict, key)
                if value is not None:
                    return value, config
            if not shared:
                # A parameter which is not shared across populations is never read from a main
                # config when it is resolved for an extra population.
                return None
        for config in self.chain():
            value = self._resolve_from_dict(config.dict, key)
            if value is not None:
                return value, config
        return None

    def resolve_parameter(
        self, key: list[str], population_name: str | None = None, shared: bool = False
    ):
        """Returns the value associated to the given key in the config (see `_lookup_parameter`).

        The value is passed through `resolve_indirection` (so `"secret:"` / `"env:"` values are
        handled).

        Returns None if the value is not defined.
        """
        value, _ = self.resolve_parameter_with_origin(key, population_name, shared=shared)
        return value

    def resolve_parameter_with_origin(
        self, key: list[str], population_name: str | None = None, shared: bool = False
    ) -> tuple[Any, Config]:
        """Same as `resolve_parameter`, also returning the config the value was written in.

        `Parameter.from_config` needs that config to resolve the value's relative paths against
        *its* directory, so that a path written in a base config resolves to the same absolute
        path as it did in the base run (otherwise `Step.config_hash` would change and the step
        would be re-run for no reason).

        The origin config is also the one resolving the `"secret:"` indirections, so that a secret
        used by a base config is read from the secrets file of *that* config.

        Returns `(None, self)` if the value is not defined, so that callers can use the returned
        config unconditionally.
        """
        found = self._lookup_parameter(key, population_name, shared=shared)
        if found is None:
            return None, self
        value, origin = found
        # Note. The lookup above tests the *raw* value, so a key which is defined but whose
        # `"secret:"` indirection cannot be resolved yields None here instead of falling back to
        # the base config: a value which is declared but invalid must be reported, not inherited.
        return origin.resolve_indirection(value), origin

    def resolve_indirection(self, value: Any) -> Any:
        """Resolves a `"secret:skey"` / `"env:var"` string indirection to its actual value.

        If the value is of the form `"secret:skey"`, returns the value associated to `skey` in the
        secrets instead.

        If the value is of the form `"env:var"`, returns the value associated to the environment
        variable `var` instead.

        Any other value (including `None`) is returned unchanged.
        """
        if isinstance(value, str) and value.startswith("secret:"):
            skey = value.removeprefix("secret:")
            return self.secrets.get(skey)
        elif isinstance(value, str) and value.startswith("env:"):
            var = value.removeprefix("env:")
            return os.environ.get(var)
        return value

    def resolve_path(self, value: Any) -> Any:
        """Resolves a relative path-like `value` against the directory of the main config file.

        Returns a `Path` when `value` is a `str`/`Path`, converting it if needed; any other value
        is returned unchanged. An absolute path is returned as-is.
        """
        if not isinstance(value, (str, Path)):
            return value
        path = Path(value)
        if self.main_path is not None and not path.is_absolute():
            return self.main_path.parent / path
        return path

    @staticmethod
    def _resolve_from_dict(d: dict[str, Any], key: list[str]) -> Any:
        """Walks `d` following the dotted `key`, returning None if any segment is not found."""
        value: Any = d
        for k in key:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return None
        return value

    def check_unused_keys(self, used_keys: set[str]):
        # Main config.
        unused_keys = self.get_unused_keys(used_keys)
        if unused_keys:
            logger.warning("The following keys appear in the main configuration but are not used:")
            for k in sorted(unused_keys):
                logger.warning(f"- {k}")
        # Extra populations.
        # Note. Only the populations declared by *this* config are checked (not
        # `population_names`): a base config's keys were already checked when that config was run,
        # and `get_unused_keys` reads `extra_populations_dict`, which does not hold them anyway.
        for pop_name in self.extra_populations_dict.keys():
            unused_keys = self.get_unused_keys(used_keys, population=pop_name)
            if unused_keys:
                logger.warning(
                    f"The following keys appear in the configuration for population `{pop_name}` "
                    "but are not used:"
                )
                for k in sorted(unused_keys):
                    logger.warning(f"- {k}")

    def get_unused_keys(self, used_keys: set[str], population: str | None = None) -> set[str]:
        """Returns a set of all keys (flatten) in the configuration that are not in `used_keys`."""
        if population is None:
            used_keys |= {
                MAIN_DIR_KEY,
                SECRETS_KEY,
                POPULATIONS_KEY,
                MAIN_POPULATION_KEY,
                CUSTOM_STEPS_KEY,
                BASE_CONFIG_KEY,
            }
            d = self.dict
        else:
            used_keys.add(POP_NAME_KEY)
            d = self.extra_populations_dict[population]
        return get_unused_keys_inner(d, set(), root=None, used_keys=used_keys)


def get_unused_keys_inner(
    d: dict[str, Any], unused_keys: set[str], root: str | None, used_keys: set[str]
) -> set[str]:
    for k, v in d.items():
        if root is None:
            flat_key = k
        else:
            flat_key = f"{root}.{k}"
        if isinstance(v, dict) and flat_key not in used_keys:
            unused_keys = get_unused_keys_inner(v, unused_keys, root=flat_key, used_keys=used_keys)
        else:
            if flat_key not in used_keys:
                unused_keys.add(flat_key)
    return unused_keys
