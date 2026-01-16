import hashlib
import json
import os
from collections.abc import Callable
from typing import Optional

import numpy as np

from pymetropolis.metro_common.errors import error_context

from .config import Config, ConfigValue, InputFile
from .file import MetroFile

EPSILON = np.finfo(float).eps

# TODO: Add something to measure running time for each step.


class Step:
    def __init__(
        self,
        slug: str,
        func: Callable[[Config], bool],
        required_files: Optional[list[MetroFile]] = None,
        optional_files: Optional[list[MetroFile]] = None,
        output_files: Optional[list[MetroFile]] = None,
        config_values: Optional[list[ConfigValue]] = None,
    ):
        self.slug = slug
        self.func = func
        self.required_files = required_files or list()
        self.optional_files = optional_files or list()
        self.output_files = output_files or list()
        self.config_values = config_values or list()
        for f in self.output_files:
            f.add_provider(self)

    def __str__(self) -> str:
        return self.slug

    def is_defined(self, config: Config) -> bool:
        """Returns `True` if this step is properly defined in the config."""
        for value in self.config_values:
            if not config.has_value(value):
                return False
        return True

    @error_context(msg="Failed to execute step `{}`", fmt_args=[0])
    def execute(self, config: Config) -> bool:
        success = self.func(config)
        if success:
            self.save_update_dict(config)
        return success

    def input_files(self) -> list[InputFile]:
        return list(filter(lambda v: v.is_file(), self.config_values))

    def required_dependencies(self, config) -> list["Step"]:
        deps = list()
        for f in self.required_files:
            deps.append(f.provider(config))
        return deps

    def optional_dependencies(self, config) -> list["Step"]:
        deps = list()
        for f in self.optional_files:
            provider = f.provider(config, optional=True)
            if provider is not None:
                deps.append(provider)
        return deps

    def update_required(self, config: Config) -> bool:
        """Returns `False` if the step was already executed and does not need to be executed again.

        A step needs to be executed again if:
        - The update file does not exist (the step has never be run).
        - Any configuration variable has been modified.
        - Any InputFile has been modified.
        - Any input MetroFile has been modified.
        - Any output MetroFile has been deleted / modified.
        """
        update_dict = self.update_dict(config)
        if update_dict is None:
            # Step has never been executed or the update file has been removed.
            return True
        # Check that the input data files have not been modified.
        for input_file in self.input_files():
            slug = input_file.slug
            if not config.has_value(input_file):
                # Input file is not specified.
                continue
            path = config[input_file]
            if not os.path.isfile(path) and update_dict.get(f"{slug}_mtime") is not None:
                # A file that was previously read no longer exists.
                return True
            if os.path.getmtime(path) != update_dict.get(f"{slug}_mtime"):
                # The file exists but was updated since the last run (or did not exist before).
                return True
        # Check that the input / output MetroFiles have not been modified.
        for f in self.required_files + self.optional_files + self.output_files:
            slug = f.slug
            path = f.get_path(config)
            if not os.path.isfile(path):
                # The file does not exists...
                if update_dict.get(f"{slug}_mtime") is None:
                    # but it's fine since it never existed.
                    continue
                else:
                    # it has been removed.
                    return True
            if os.path.getmtime(path) != update_dict.get(f"{slug}_mtime"):
                # The file exists but was updated since the last run (or did not exist before).
                return True
        # Check that the relevant config has not been modified.
        if self.config_hash(config) != update_dict.get("config_hash"):
            return True
        return False

    def update_dict(self, config: Config) -> dict | None:
        """Returns a dictionary representing the update file of this step.

        Returns `None` if the update file does not exist.
        """
        filename = config.update_dict_path(self.slug)
        if os.path.isfile(filename):
            with open(filename, "r") as f:
                return json.load(f)
        else:
            return None

    def config_hash(self, config: Config):
        """Returns a hash of the config relevant for the step."""
        config_dict = dict()
        for value in self.config_values:
            config_dict[value.slug] = config.get(value)
        # default=str is required to dump datetime variables
        json_str = json.dumps(config_dict, sort_keys=True, default=str)
        h = hashlib.sha256()
        h.update(json_str.encode())
        return h.hexdigest()

    def save_update_dict(self, config: Config):
        """Saves a dictionary representing the update file of this step."""
        update_dict = dict()
        for input_file in self.input_files():
            slug = input_file.slug
            if not config.has_value(input_file):
                # Input file is not specified.
                continue
            filename = config[input_file]
            if not os.path.isfile(filename):
                # Input file is not specified.
                continue
            update_dict[f"{slug}_mtime"] = os.path.getmtime(filename)
        for f in self.required_files + self.optional_files + self.output_files:
            slug = f.slug
            filename = f.get_path(config)
            if not os.path.isfile(filename):
                continue
            update_dict[f"{slug}_mtime"] = os.path.getmtime(filename)
        update_dict["config_hash"] = self.config_hash(config)
        filename = config.update_dict_path(self.slug)
        # Create directory if needed.
        directory = os.path.dirname(filename)
        if not os.path.isdir(directory):
            os.makedirs(directory)
        with open(filename, "w") as f:
            json.dump(update_dict, f)
