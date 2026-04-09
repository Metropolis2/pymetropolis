import os
import tomllib
from pathlib import Path
from typing import Any

from pymetropolis.metro_common import MetropyError


class Config:
    main_directory: Path

    def __init__(self, d: dict):
        self.dict = d

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
        inst.check_main_directory()
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
