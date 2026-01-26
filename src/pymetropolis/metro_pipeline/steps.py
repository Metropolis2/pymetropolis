import hashlib
import json
from itertools import chain
from pathlib import Path
from typing import Any, Type

import numpy as np

from pymetropolis.metro_common.errors import MetropyError, error_context

from .config import Config
from .file import MetroFile
from .parameters import IntParameter, Parameter

# TODO: Add something to measure running time for each step.


class Step:
    output_files: dict[str, Type[MetroFile]]
    _input_files: dict[str, MetroFile]
    _output_files: dict[str, MetroFile]
    _update_file_path: Path
    _config_dict: dict[str, Any]

    def __init__(self, config: Config):
        self._config_dict = dict()
        for param_name in dir(self.__class__):
            param_obj = getattr(self.__class__, param_name)
            if not isinstance(param_obj, Parameter):
                continue
            value = param_obj.from_config(config)
            self._config_dict[param_name] = value
            setattr(self, param_name, value)
        self._output_files = {
            k: f.from_dir(config.main_directory) for k, f in self.output_files.items()
        }
        self._input_files = {
            k: f.from_dir(config.main_directory) for k, f in self.required_files().items()
        }
        self._input_files.update(
            {k: f.from_dir(config.main_directory) for k, f in self.optional_files().items()}
        )
        self._update_file_path = config.main_directory / "update_files" / f"{self}.json"

    def required_files(self) -> dict[str, Type[MetroFile]]:
        return dict()

    def optional_files(self) -> dict[str, Type[MetroFile]]:
        return dict()

    def __str__(self) -> str:
        return self.__class__.__name__

    def run(self):
        """Executes the step.

        This method needs to be overridden by each subclass.
        """
        raise MetropyError(f"Step {self} has not `run` implementation.")

    @property
    def input(self) -> dict[str, MetroFile]:
        return self._input_files

    @property
    def output(self) -> dict[str, MetroFile]:
        return self._output_files

    def is_defined(self) -> bool:
        """Returns `True` if this step is properly defined in the config."""
        return True

    @error_context(msg="Failed to execute step `{}`", fmt_args=[0])
    def execute(self, config: Config):
        self.run()
        self.save_update_dict(config)

    def update_required(self) -> bool:
        """Returns `False` if the step was already executed and does not need to be executed again.

        A step needs to be executed again if:
        - The update file does not exist (the step has never be run).
        - Any configuration variable has been modified.
        - Any InputFile has been modified.
        - Any input MetroFile has been modified.
        - Any output MetroFile has been deleted / modified.
        """
        update_dict = self.update_dict()
        if update_dict is None:
            # Step has never been executed or the update file has been removed.
            return True
        # Check that the input data files have not been modified.
        for k, v in self._config_dict.items():
            if not isinstance(v, Path):
                continue
            if not v.exists() and update_dict.get(f"data_file_{k}_mtime") is not None:
                # A file that was previously read no longer exists.
                return True
            if v.stat().st_mtime != update_dict.get(f"data_file_{k}_mtime"):
                # The file exists but was updated since the last run (or did not exist before).
                return True
        # Check that the input / output MetroFiles have not been modified.
        for k, f in chain(self.input.items(), self.output.items()):
            if not f.exists():
                # The file does not exists...
                if update_dict.get(f"metro_file_{k}_mtime") is None:
                    # but it's fine since it never existed.
                    continue
                else:
                    # it has been removed.
                    return True
            if f.last_modified_time() != update_dict.get(f"metro_file_{k}_mtime"):
                # The file exists but was updated since the last run (or did not exist before).
                return True
        # Check that the relevant config has not been modified.
        if self.config_hash() != update_dict.get("config_hash"):
            return True
        return False

    def update_dict(self) -> dict | None:
        """Returns a dictionary representing the update file of this step.

        Returns `None` if the update file does not exist.
        """
        if self._update_file_path.is_file():
            with open(self._update_file_path, "r") as f:
                return json.load(f)
        else:
            return None

    def config_hash(self) -> str:
        """Returns a hash of the config relevant for the step."""
        # default=str is required to dump datetime variables
        json_str = json.dumps(self._config_dict, sort_keys=True, default=str)
        h = hashlib.sha256()
        h.update(json_str.encode())
        return h.hexdigest()

    def save_update_dict(self, config: Config):
        """Saves a dictionary representing the update file of this step."""
        update_dict = dict()
        for k, v in self._config_dict.items():
            if not isinstance(v, Path):
                continue
            if not v.exists():
                # Input file is not specified.
                continue
            update_dict[f"data_file_{k}_mtime"] = v.stat().st_mtime
        for k, f in chain(self.input.items(), self.output.items()):
            if not f.exists():
                continue
            update_dict[f"metro_file_{k}_mtime"] = f.last_modified_time()
        update_dict["config_hash"] = self.config_hash()
        with open(self._update_file_path, "w") as f:
            json.dump(update_dict, f)


class MetroStep(Step):
    random_seed = IntParameter(
        "random_seed",
        description="Random seed used to initialize the random number generator.",
        note=(
            "If the random seed is not defined, some operations are not deterministic, i.e., they can "
            "produce different results if re-run."
        ),
    )

    def get_rng(self) -> np.random.Generator:
        return np.random.default_rng(self.random_seed)
