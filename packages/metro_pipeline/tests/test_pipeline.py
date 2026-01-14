from metro_pipeline import Config, RunOrder, Step


class Optional(Step):
    """An optional step that is only run when the `optional-step` key exists in the config."""

    _SLUG = "optional"

    def is_defined(self, config: Config) -> bool:
        # This step is executed only if the "optional-step" section is defined in the config.
        return config.has_key("optional")

    def execute(self, config: Config) -> bool:
        print("Executing Optional step")
        return True


class ProviderA(Step):
    """A step that optionally requires the `Optional` step and that provides `Provided`."""

    _SLUG = "provider-a"

    OPTIONAL_DEPENDENCIES = [Optional()]

    def is_defined(self, config: Config) -> bool:
        # This step is executed only if the "provider-a" section is defined in the config.
        return config.has_key("provider-a")

    def execute(self, config: Config) -> bool:
        print("Executing Provider-A step")
        return True


class ProviderB(Step):
    """A step that provides `Provided`."""

    _SLUG = "provider-b"

    def is_defined(self, config: Config) -> bool:
        # This step is executed only if the "provider-b" section is defined in the config.
        return config.has_key("provider-b")

    def execute(self, config: Config) -> bool:
        print("Executing Provider-B step")
        return True


class MyProvider(Step):
    """A provider that requires either `ProviderA` or `ProviderB` to be run."""

    _SLUG = "my-provider"

    PROVIDERS = [ProviderA(), ProviderB()]


class Final(Step):
    """A step that requires `Optional` to be run and `MyProvider` to be provided."""

    _SLUG = "final"

    DEPENDENCIES = [Optional(), MyProvider()]

    def execute(self, config: Config) -> bool:
        print("Executing Final step")
        return True


STEP_DICT = {
    "optional": Optional,
    "provider-a": ProviderA,
    "provider-b": ProviderB,
    "my-provider": MyProvider,
}


def test1():
    config = {
        "optional": {},
        "provider-a": {},
    }
    order = RunOrder(MyProvider(), Config(config)).order
    expected = ["optional", "provider-a", ""]
    print(order)
    assert order == expected
