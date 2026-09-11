from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_demand.population import TripsFile, UniformDrawsFile
from pymetropolis.metro_pipeline import PopulationStep, Step
from pymetropolis.metro_pipeline.parameters import EnumParameter, FloatParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_simulation.common import merge_populations
from pymetropolis.modes import StepWithModes

from .files import (
    MetroAgentsFile,
    MetroAgentsPopulationFile,
    MetroExAnteAgentsFile,
    MetroExAnteAgentsPopulationFile,
    MetroExAnteAlternativesPopulationFile,
)


class PrepareMetroAgentsStep(StepWithModes, PopulationStep):
    """Prepares the input agents for the Metropolis-Core simulation.

    If mode choice is enabled (more than 1 mode is simulated), the mode-choice parameters of the
    agents are initiated.
    """

    # TODO: Implement and explain in the docs the 4 mode choice models.
    mode_choice_model = EnumParameter(
        "mode_choice.model",
        values=["Logit", "DrawnLogit", "DrawnNestedLogit", "Deterministic"],
        default="Deterministic",
        description="Type of choice model for mode choice",
    )
    mode_choice_mu = FloatParameter(
        "mode_choice.mu",
        default=1.0,
        description="Value of mu for the Logit choice model",
        note="Only required when mode choice model is Logit",
    )
    input_files = {
        "trips": TripsFile,
        "uniform_draws": InputFile(
            UniformDrawsFile,
            when=lambda inst: inst.has_mode_choice(),
            when_doc="if there are at least two modes",
        ),
    }
    output_files = {"agents": MetroAgentsPopulationFile}

    def is_defined(self) -> bool:
        return self.modes is not None and (
            not self.has_mode_choice() or self.mode_choice_model is not None
        )

    def run(self):
        import polars as pl

        trips = self.input["trips"].read()
        agents = trips.select(agent_id="tour_id").unique().sort("agent_id")
        if self.has_mode_choice():
            # Add mode choice parameters.
            model = self.mode_choice_model
            if model == "Logit":
                agents = agents.with_columns(
                    pl.lit("Logit").alias("alt_choice.type"),
                    pl.lit(self.mode_choice_mu).alias("alt_choice.mu"),
                )
            elif model == "DrawnLogit":
                # TODO: At this point the epsilons should be already drawn.
                raise MetropyError("TODO")
            elif model == "DrawnNestedLogit":
                raise MetropyError("TODO")
            elif model == "Deterministic":
                agents = agents.with_columns(pl.lit("Deterministic").alias("alt_choice.type"))
            draws = self.input["uniform_draws"].read()
            agents = agents.join(
                draws.select(pl.col("mode_u").alias("alt_choice.u"), agent_id="tour_id"),
                on="agent_id",
                how="left",
            )
        self.output["agents"].write(agents)


class PrepareExAnteMetroAgentsStep(StepWithModes, PopulationStep):
    """Prepares the input agents for the Metropolis-Core simulation with modes and departure times
    fixed to their ex-ante values.

    Each trip corresponds to one agent in the simulation.

    Trips with no ex-ante departure time are removed.
    """

    input_files = {"alts": MetroExAnteAlternativesPopulationFile}
    output_files = {"agents": MetroExAnteAgentsPopulationFile}
    priority = 0

    def run(self):
        alts = self.input["alts"].read()
        agents = alts.select("agent_id").unique(maintain_order=True)
        self.output["agents"].write(agents)


class WriteMetroAgentsStep(Step):
    """Merges the agents in each population and writes the agents input file for Metropolis-Core."""

    input_files = {"population_agents": InputFile(MetroAgentsPopulationFile, all_populations=True)}
    output_files = {"metro_agents": MetroAgentsFile}

    def run(self):
        agents = merge_populations(self.input_populations["population_agents"])
        self.output["metro_agents"].write(agents)


class WriteExAnteMetroAgentsStep(Step):
    """Merges the agents in each population and writes the agents input file for the ex-ante
    simulation.
    """

    input_files = {
        "population_agents": InputFile(MetroExAnteAgentsPopulationFile, all_populations=True)
    }
    output_files = {"metro_agents": MetroExAnteAgentsFile}
    priority = 0

    def run(self):
        agents = merge_populations(self.input_populations["population_agents"])
        self.output["metro_agents"].write(agents)
