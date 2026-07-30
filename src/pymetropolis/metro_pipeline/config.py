import os
import tomllib
from pathlib import Path
from typing import Any

from loguru import logger

from pymetropolis.metro_common import MetropyError


class Config:
    main_directory: Path
    secrets: dict

    def __init__(self, d: dict):
        self.dict = d
        self.check_main_directory()
        self.read_secrets()

    @classmethod
    def from_toml(cls, path: Path):
        """Initializes a Config from the path to a TOML file.

        Raises an exception if the given filename does not exist or is an invalid TOML file.
        """
        if not os.path.isfile(path):
            raise MetropyError(f"Cannot read config file: {os.path.abspath(path)}")
        with open(path, "rb") as f:
            input_dict = tomllib.load(f)
        inst = cls(input_dict)
        return inst

    def check_main_directory(self):
        """Asserts that `main_directory` is properly defined and that the directory exists.

        If the directory does not exist, creates it.
        """
        main_dir = self.dict.get("main_directory")
        if main_dir is None:
            raise MetropyError("Missing `main_directory` in config")
        if not isinstance(main_dir, str):
            raise MetropyError(f"Config value `main_directory` should be a path, got `{main_dir}`")
        path = Path(main_dir)
        path.mkdir(exist_ok=True, parents=True)
        self.main_directory = path
        # Also create the update_files/ directory if needed.
        update_files_path = path / "update_files"
        update_files_path.mkdir(exist_ok=True)

    def read_secrets(self):
        """Reads the secrets file if it exists.

        If the `secrets_file` config is not defined, the default path is `secrets.toml`.
        """
        secrets_file_def = self.dict.get("secrets_file")
        if secrets_file_def is not None:
            if not isinstance(secrets_file_def, str):
                raise MetropyError(
                    f"Invalid `secrets_file` parameter: Not a path: `{secrets_file_def}`"
                )
            if not Path(secrets_file_def).exists():
                raise MetropyError(
                    f"Invalid `secrets_file` parameter: Path `{secrets_file_def}` does not exist"
                )
        # When not specified, default path is `secrets.toml`.
        secrets_file = secrets_file_def or "secrets.toml"
        path = Path(secrets_file)
        if path.exists():
            with open(path, "rb") as f:
                self.secrets = tomllib.load(f)
        else:
            # Do not raise an error when the default file path does not exist.
            logger.debug(f"Secrets file path does not exist: `{path}`")
            self.secrets = dict()

    def resolve_parameter(self, key: list[str]):
        """Returns the value associated to the given key in the config.

        If the value if of the form `"secret:skey"`, returns the value associated to `skey` in the
        secrets instead.

        If the value if of the form `"env:var"`, returns the value associated to the environnement
        variable `var` instead.

        Returns None if the value is not defined.
        """
        value = self.dict
        for k in key:
            if k in value:
                value = value[k]
            else:
                # The key is not defined in the config.
                return None
        # At this point, the key was found and `value` is equal to its value.
        if isinstance(value, str) and value.startswith("secret:"):
            key = value.removeprefix("secret:")
            value = self.secrets.get(key)
        elif isinstance(value, str) and value.startswith("env:"):
            var = value.removeprefix("env:")
            value = os.environ.get(var)
        return value

    def get_unused_keys(self, used_keys: set[str]) -> set[str]:
        """Returns a set of all keys (flatten) in the configuration that are not in `used_keys`."""
        used_keys.add("main_directory")
        return get_unused_keys_inner(self.dict, set(), root=None, used_keys=used_keys)


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
