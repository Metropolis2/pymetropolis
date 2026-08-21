from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Self

from pymetropolis.metro_common.errors import MetropyError, error_context

from .file import MetroFile, PopulationFile
from .parameters import Parameter, PathParameter

if TYPE_CHECKING:
    from .config import Config

# TODO: Add something to measure running time for each step.

# Population "name" used to resolve PopulationFile paths for the main / default population, i.e.
# the population defined directly in the main config (as opposed to an extra population, defined
# in its own config file and with its own name).
MAIN_POPULATION_NAME = "population"


def _namespace(file_class: type[MetroFile], population: str) -> type[MetroFile]:
    """Returns the MetroFile class instance that correspond to the given population."""
    if issubclass(file_class, PopulationFile):
        return file_class.for_population(population)
    return file_class


class InputFile:
    def __init__(
        self,
        file_class: type[MetroFile],
        optional: bool = False,
        when: Callable[[Any], bool] | None = None,
        when_doc: str | None = None,
        all_populations: bool = False,
    ):
        if all_populations and not issubclass(file_class, PopulationFile):
            raise MetropyError(
                f"`all_populations=True` requires a PopulationFile, got `{file_class.__name__}`"
            )
        self.file_class = file_class
        self.optional = optional
        self.when = when
        self.when_doc = when_doc
        self.all_populations = all_populations

    def is_needed(self, step: Step) -> bool:
        if self.when:
            return self.when(step)
        else:
            return True

    def _md_doc(self) -> str:
        doc = f"[`{self.file_class.__name__}`](files.html#{self.file_class.__name__.lower()})"
        if self.optional:
            doc += " (optional)"
        if self.all_populations:
            doc += " (for all populations)"
        if self.when_doc:
            doc += f" [{self.when_doc}]"
        return doc


class Step:
    input_files: ClassVar[dict[str, InputFile | type[MetroFile]]] = {}
    output_files: ClassVar[dict[str, type[MetroFile]]] = {}
    priority: ClassVar[int] = 1
    # Set to True on PopulationStep.
    _is_population_step: ClassVar[bool] = False
    _input_files: dict[str, MetroFile]
    # Resolved instances for `all_populations` inputs, keyed by input name then population name.
    # Kept separate from `_input_files` so `self.input[name]` stays a plain MetroFile everywhere.
    _population_input_files: dict[str, dict[str, MetroFile]]
    _output_files: dict[str, MetroFile]
    _update_file_path: Path
    _config_dict: dict[str, Any]
    _data_files: dict[str, Path]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if cls._is_population_step:
            # A PopulationStep is instantiated once per population, so a non-PopulationFile output
            # would resolve to the identical path for every population: the pipeline's conflict
            # resolution would then silently keep only one population's run, discarding the rest.
            # (Non-PopulationFile inputs are fine: reading the same shared/global file identically
            # for every population is a normal pattern.)
            for name, file_class in cls.output_files.items():
                if not issubclass(file_class, PopulationFile):
                    raise MetropyError(
                        f"`{cls.__name__}` is a PopulationStep but declares output `{name}` as "
                        f"`{file_class.__name__}`, which is not a PopulationFile: every "
                        "population's instance would write to the identical path, and the "
                        "pipeline would silently keep only one population's result."
                    )
            return
        # A Step that is not a PopulationStep is only ever instantiated once, resolved against the
        # main population, so a PopulationFile input/output would silently only ever see the main
        # population's copy, ignoring every extra population. The only sanctioned way for such a
        # Step to touch a PopulationFile is an `all_populations=True` input.
        for name, spec in cls.input_files.items():
            file_class = spec.file_class if isinstance(spec, InputFile) else spec
            if issubclass(file_class, PopulationFile) and not (
                isinstance(spec, InputFile) and spec.all_populations
            ):
                raise MetropyError(
                    f"`{cls.__name__}` is not a PopulationStep but declares input `{name}` as "
                    f"PopulationFile `{file_class.__name__}` without `all_populations=True`: it "
                    "would silently only ever see the main population's copy."
                )
        for name, file_class in cls.output_files.items():
            if issubclass(file_class, PopulationFile):
                raise MetropyError(
                    f"`{cls.__name__}` is not a PopulationStep but declares output `{name}` as "
                    f"PopulationFile `{file_class.__name__}`: it would always write only to the "
                    "main population's path. Make it a PopulationStep instead."
                )

    def __init__(self, config: Config):
        self._init_from_config(config)

    def _init_from_config(self, config: Config, population_name: str | None = None) -> None:
        """Fills in the config-derived parameters, input/output files and cache path of the step.

        `population_name` is the extra population this step is being instantiated for, or `None`
        for the main / default population (defined directly in the main config).
        """
        self._config_dict = dict()
        self._data_files = dict()
        for param_name, param_obj in self.__class__._iter_params():
            value = param_obj.from_config(config, population_name)
            self._config_dict[param_name] = value
            setattr(self, param_name, value)
            if isinstance(param_obj, PathParameter):
                # Store path parameters so we can check whether they are tempered with.
                # Note. Executable files (metropolis_cli and routing_cli) are excluded from this
                # check since they use the ExecPathParameter class.
                # This means that switching to a new Metropolis-Core version will not trigger the
                # re-execution of the steps.
                # This also allows to switch Operating System without having to re-run steps (the
                # executables have different hashes over different OSs).
                self._data_files[param_name] = value
        file_population = population_name if population_name is not None else MAIN_POPULATION_NAME
        all_population_names = [
            *([MAIN_POPULATION_NAME] if config.main_population else []),
            *config.extra_populations_dict.keys(),
        ]
        self._input_files = {}
        self._population_input_files = {}
        for k, f in self._iter_input_files():
            spec = self.input_files[k]
            if isinstance(spec, InputFile) and spec.all_populations:
                self._population_input_files[k] = {
                    population: _namespace(f, population).from_dir(config.main_directory)
                    for population in all_population_names
                }
            else:
                self._input_files[k] = _namespace(f, file_population).from_dir(
                    config.main_directory
                )
        self._output_files = {
            k: _namespace(f, file_population).from_dir(config.main_directory)
            for k, f in self.output_files.items()
        }
        self._update_file_path = config.main_directory / "update_files" / f"{self}.json"

    @classmethod
    def _iter_params(cls):
        for param_name in dir(cls):
            param_obj = getattr(cls, param_name)
            if not isinstance(param_obj, Parameter):
                continue
            yield param_name, param_obj

    def _iter_input_files(self, required: bool | None = None):
        for name, file_spec in self.input_files.items():
            if isinstance(file_spec, InputFile):
                if required and file_spec.optional:
                    # File is optional but only required files are demanded.
                    continue
                if required is False and not file_spec.optional:
                    # File is not optional but only optional files are demanded.
                    continue
                if not file_spec.is_needed(self):
                    # File is not used given the configured parameters.
                    continue
                yield name, file_spec.file_class
            else:
                if required is False:
                    # File is not optional but only optional files are demanded.
                    continue
                yield name, file_spec

    def _iter_resolved_input_files(self, required: bool | None = None) -> Iterator[MetroFile]:
        """Yields each resolved MetroFile instance for inputs matching `required`.

        For an `all_populations` input, yields each population's instance individually.
        """
        for name, _ in self._iter_input_files(required=required):
            if name in self._population_input_files:
                yield from self._population_input_files[name].values()
            else:
                yield self.input[name]

    def _iter_flat_files(self) -> Iterator[tuple[str, MetroFile]]:
        """Yields (key, MetroFile) for every input/output file, including one entry per
        population (with a synthesized `name__population` key) for `all_populations` inputs.
        """
        yield from self.input.items()
        yield from self.output.items()
        for name, pop_files in self._population_input_files.items():
            for population, f in pop_files.items():
                yield f"{name}__{population}", f

    def __str__(self) -> str:
        return self.__class__.__name__

    def __lt__(self, other) -> bool:
        """Compares `self` to `other`.

        The ordering is based on:

        1. The `priority` attribute.
        2. The number of output files.
        3. Class name.
        """
        if self.priority != other.priority:
            return self.priority < other.priority
        if len(self.output) != len(other.output):
            return len(self.output) < len(other.output)
        return str(self) < str(other)

    def run(self):
        """Executes the step.

        This method needs to be overridden by each subclass.
        """
        raise MetropyError(f"Step {self} has not `run` implementation.")

    @property
    def input(self) -> dict[str, MetroFile]:
        return self._input_files

    @property
    def input_populations(self) -> dict[str, dict[str, MetroFile]]:
        """For each input declared with `InputFile(..., all_populations=True)`, the resolved
        MetroFile instance for each population, keyed by population name.
        """
        return self._population_input_files

    @property
    def output(self) -> dict[str, MetroFile]:
        return self._output_files

    def is_defined(self) -> bool:
        """Returns `True` if this step is properly defined in the config."""
        return True

    def is_primary(self) -> bool:
        """Returns whether the Step is a "primary" step.

        Primary steps are steps whose priority is greater than 0.

        A non-primary step is run only if its output is required for another primary step.
        """
        return self.priority > 0

    @error_context(msg="Failed to execute step `{}`", fmt_args=[0])
    def execute(self):
        self.run()
        self.save_update_dict()

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
        for k, v in self._data_files.items():
            if v is None:
                continue
            if not v.exists() and update_dict.get(f"data_file_{k}_mtime") is not None:
                # A file that was previously read no longer exists.
                return True
            # TODO: Handle data directories.
            if v.stat().st_mtime != update_dict.get(f"data_file_{k}_mtime"):
                # The file exists but was updated since the last run (or did not exist before).
                return True
        # Check that the input / output MetroFiles have not been modified.
        for k, f in self._iter_flat_files():
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
        return self.config_hash() != update_dict.get("config_hash")

    def update_dict(self) -> dict | None:
        """Returns a dictionary representing the update file of this step.

        Returns `None` if the update file does not exist.
        """
        if self._update_file_path.is_file():
            with open(self._update_file_path, encoding="utf-8") as f:
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

    def save_update_dict(self):
        """Saves a dictionary representing the update file of this step."""
        update_dict = dict()
        for k, v in self._data_files.items():
            if v is None or not v.exists():
                # Input file is not specified.
                continue
            update_dict[f"data_file_{k}_mtime"] = v.stat().st_mtime
        for k, f in self._iter_flat_files():
            if not f.exists():
                continue
            update_dict[f"metro_file_{k}_mtime"] = f.last_modified_time()
        update_dict["config_hash"] = self.config_hash()
        with open(self._update_file_path, "w", encoding="utf-8") as f:
            json.dump(update_dict, f)

    @classmethod
    def _md_doc(cls) -> str:
        doc = f"## {cls.__name__}\n\n"
        if cls.__doc__:
            doc += cls.__doc__
            # There is a non-breakable whitespace there to properly split docstrings finishing by a
            # list from the following list.
            doc += "\n&nbsp;\n"
        doc += cls._md_doc_params()
        doc += cls._md_doc_input_files()
        doc += cls._md_doc_output_files()
        return doc

    @classmethod
    def _md_doc_params(cls) -> str:
        params = list()
        for param_name, param_obj in cls._iter_params():
            key_str = ".".join(param_obj.key)
            # Hash is the string that must be used to properly link to the parameters page.
            hash = "".join(param_obj.key)
            params.append(f"[`{key_str}`](parameters.html#{hash})")
        if params:
            doc = "\n- **Parameters:** " + ", ".join(sorted(params)) + "\n"
            return doc
        else:
            # There is not parameter.
            return ""

    @classmethod
    def _md_doc_input_files(cls) -> str:
        files = list()
        for ifile in cls.input_files.values():
            if isinstance(ifile, InputFile):
                files.append(ifile._md_doc())
            else:
                files.append(f"[`{ifile.__name__}`](files.html#{ifile.__name__.lower()})")
        if files:
            doc = "\n- **Input files:** " + ", ".join(sorted(files)) + "\n"
            return doc
        else:
            # There is not output file.
            return ""

    @classmethod
    def _md_doc_output_files(cls) -> str:
        files = list()
        for ofile in cls.output_files.values():
            files.append(f"[`{ofile.__name__}`](files.html#{ofile.__name__.lower()})")
        if files:
            doc = "\n- **Output files:** " + ", ".join(sorted(files)) + "\n"
            return doc
        else:
            # There is not output file.
            return ""


class PopulationStep(Step):
    """Abstract Step to define processing steps which might be run for each population defined."""

    _is_population_step: ClassVar[bool] = True

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "__init__" in cls.__dict__:
            raise MetropyError(
                f"`{cls.__name__}` must not override `__init__`: `for_population` builds extra-"
                "population instances with `cls.__new__(cls)` followed by `_init_from_config`, "
                "bypassing `__init__` entirely, so any override would silently only run for the "
                "main population."
            )

    # None for the main / default population, defined in the main config. Set to the population
    # name for extra populations (see `for_population`).
    population_name: str = MAIN_POPULATION_NAME

    def __str__(self) -> str:
        if self.population_name != MAIN_POPULATION_NAME:
            return f"{self.population_name}__{super().__str__()}"
        return super().__str__()

    @classmethod
    def for_population(cls, config: Config, population_name: str) -> Self:
        instance = cls.__new__(cls)
        instance.population_name = population_name
        instance._init_from_config(config, population_name)
        return instance
