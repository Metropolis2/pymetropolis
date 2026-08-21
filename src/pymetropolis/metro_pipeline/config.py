import os
import tomllib
from pathlib import Path
from tomllib import TOMLDecodeError
from typing import Any

from loguru import logger

from pymetropolis.metro_common import MetropyError

from .steps import MAIN_POPULATION_NAME, PopulationStep, Step

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
    dict: dict[str, Any]
    extra_populations_dict: dict[str, dict]
    secrets: dict[str, Any]
    main_population: bool
    custom_step_paths: list[Path]

    def __init__(self, d: dict, main_path: Path | None = None):
        self.dict = d
        self.check_main_directory()
        self.read_secrets()
        self.read_main_population()
        self.read_extra_populations(main_path)
        self.read_custom_steps(main_path)

    @classmethod
    def from_toml(cls, path: Path):
        """Initializes a Config from the path to a TOML file.

        Raises an exception if the given filename does not exist or is an invalid TOML file.
        """
        input_dict = parse_toml(path)
        inst = cls(input_dict, path)
        return inst

    def check_main_directory(self):
        """Asserts that `main_directory` is properly defined and that the directory exists.

        If the directory does not exist, creates it.
        """
        main_dir = self.dict.get(MAIN_DIR_KEY)
        if main_dir is None:
            raise MetropyError(f"Missing `{MAIN_DIR_KEY}` in config")
        if not isinstance(main_dir, str):
            raise MetropyError(f"Config value `{MAIN_DIR_KEY}` should be a path, got `{main_dir}`")
        path = Path(main_dir)
        path.mkdir(exist_ok=True, parents=True)
        self.main_directory = path
        # Also create the update_files/ directory if needed.
        update_files_path = path / "update_files"
        update_files_path.mkdir(exist_ok=True)

    def read_secrets(self):
        """Reads the secrets file if it exists.

        If the SECRETS_KEY config key is not defined, the default path is `secrets.toml`.
        """
        secrets_file_def = self.dict.get(SECRETS_KEY)
        if secrets_file_def is not None:
            if not isinstance(secrets_file_def, str):
                raise MetropyError(
                    f"Invalid `{SECRETS_KEY}` parameter: Not a path: `{secrets_file_def}`"
                )
            if not Path(secrets_file_def).exists():
                raise MetropyError(
                    f"Invalid `{SECRETS_KEY}` parameter: Path `{secrets_file_def}` does not exist"
                )
        # When not specified, default path is `secrets.toml`.
        secrets_file = secrets_file_def or "secrets.toml"
        path = Path(secrets_file)
        if path.exists():
            self.secrets = parse_toml(path)
        else:
            # Do not raise an error when the default file path does not exist.
            logger.debug(f"Secrets file path does not exist: `{path}`")
            self.secrets = dict()

    def read_extra_populations(self, main_path: Path | None):
        """Reads the configuration files for the extra populations, if they are defined."""
        populations = self.dict.get(POPULATIONS_KEY)
        self.extra_populations_dict = dict()
        used_names = set()
        if self.main_population:
            used_names.add(MAIN_POPULATION_NAME)
        if not populations:
            # Not population defined, or only a "standard" population.
            return
        for pop_config in populations:
            try:
                rel_path = Path(pop_config)
            except (TypeError, ValueError):
                raise MetropyError(f"Invalid population config file: Not a path: `{pop_config}`")
            if main_path is not None:
                # Path is relative to the config file.
                path = main_path.parent / rel_path
            else:
                path = rel_path
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

    def read_main_population(self):
        """Reads whether the main / default population (defined directly in the main config)
        should be used. Defaults to `True`.
        """
        value = self.dict.get(MAIN_POPULATION_KEY, True)
        if not isinstance(value, bool):
            raise MetropyError(f"`{MAIN_POPULATION_KEY}` parameter should be a boolean: `{value}`")
        self.main_population = value

    def read_custom_steps(self, main_path: Path | None):
        """Reads the paths to the Python files defining custom Step classes, if any are defined.

        Paths are resolved relative to the main config file.
        """
        custom_steps = self.dict.get(CUSTOM_STEPS_KEY)
        self.custom_step_paths = list()
        if not custom_steps:
            return
        for custom_step in custom_steps:
            try:
                rel_path = Path(custom_step)
            except (TypeError, ValueError):
                raise MetropyError(f"Invalid custom step file: Not a path: `{custom_step}`")
            if main_path is not None:
                # Path is relative to the config file.
                path = main_path.parent / rel_path
            else:
                path = rel_path
            if not path.is_file():
                raise MetropyError(f"Custom step file does not exist: `{path}`")
            self.custom_step_paths.append(path)

    def instantiate_step(self, step_class: type[Step]) -> list[Step]:
        steps = list()
        if not issubclass(step_class, PopulationStep) or self.main_population:
            steps.append(step_class(self))
        if issubclass(step_class, PopulationStep):
            for pop_name in self.extra_populations_dict.keys():
                steps.append(step_class.for_population(self, pop_name))
        return steps

    def resolve_parameter(
        self, key: list[str], population_name: str | None = None, shared: bool = False
    ):
        """Returns the value associated to the given key in the config.

        If `population_name` is given, the key is first looked up in that population's own config
        file. If it is not defined there, it is only read from the main config when `shared` is
        `True`, i.e., the parameter is declared as being shared across populations.

        If the value is of the form `"secret:skey"`, returns the value associated to `skey` in the
        secrets instead.

        If the value is of the form `"env:var"`, returns the value associated to the environnement
        variable `var` instead.

        Returns None if the value is not defined.
        """
        value = None
        if population_name is not None:
            value = self._resolve_from_dict(self.extra_populations_dict[population_name], key)
            if value is None and not shared:
                return None
        if value is None:
            value = self._resolve_from_dict(self.dict, key)
        # At this point, `value` is equal to the resolved value (or None if it is not defined).
        if isinstance(value, str) and value.startswith("secret:"):
            skey = value.removeprefix("secret:")
            value = self.secrets.get(skey)
        elif isinstance(value, str) and value.startswith("env:"):
            var = value.removeprefix("env:")
            value = os.environ.get(var)
        return value

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
