import sys

import click
import matplotlib.pyplot as plt
import networkx as nx
from loguru import logger
from termcolor import colored

from pymetropolis.metro_common import logger as metro_logger

from .config import Config
from .file import MetroFile
from .steps import Step

# class RunOrder:
#     def __init__(self, step: Step, config: Config):
#         self.all_steps: set[str] = set()
#         self.mandatory_steps: set[str] = set()
#         self.undefined_steps: set[str] = set()
#         self.order: list[Step] = list()
#         self.add_dependency(step, config)

#     def add_dependency(self, step: Step, config: Config, optional: bool = False):
#         if step.slug in self.all_steps:
#             # This step (and all its dependencies) have already been added to the run order.
#             # If this step is part of the order, it means that an update is required.
#             return step.slug in self.order
#         if not step.is_defined(config):
#             # The step is not defined so it cannot be properly executed.
#             if optional:
#                 # The step is optional so it can simply be skipped.
#                 return False
#             else:
#                 # The step is mandatory so we add it to the list of step that will not be able to
#                 # run.
#                 self.undefined_steps.add(step.slug)
#         update_required = step.update_required(config)
#         for dep in step.required_dependencies(config):
#             # Recursively explore the step dependencies with a depth-first search.
#             update_required |= self.add_dependency(dep, config)
#         for dep in step.optional_dependencies(config):
#             # Recursively explore the optional step dependencies with a depth-first search.
#             update_required |= self.add_dependency(dep, config, optional=True)
#         assert step.slug not in self.all_steps
#         assert step.slug not in self.mandatory_steps
#         self.all_steps.add(step.slug)
#         if update_required:
#             self.order.append(step)
#             if not optional:
#                 self.mandatory_steps.add(step.slug)
#         return update_required

#     def pretty_order(self) -> str:
#         s = ""
#         for i, step in enumerate(self.order):
#             dep_str = f"{i + 1}. {step.slug}"
#             if step.slug in self.undefined_steps:
#                 dep_str = colored(f"{dep_str} *", "red")
#             s += dep_str + "\n"
#         if self.undefined_steps:
#             s += colored("Steps in red with an asterisk (*) are not properly defined")
#         return s


# def run_pipeline(requested_step: Step, config: Config, dry_run: bool = False):
#     metro_logger.setup()
#     run_order = RunOrder(requested_step, config)
#     if dry_run:
#         print(run_order.pretty_order())
#     else:
#         if run_order.undefined_steps:
#             for step in run_order.undefined_steps:
#                 logger.error(f"Step `{step}` is not properly defined")
#             raise MetropyError("At least one step was not properly defined")
#         n = len(run_order.order)
#         for i, step in enumerate(run_order.order):
#             logger.info(f"=== Step {i + 1} / {n}: {step.slug} ===")
#             success = step.execute(config)
#             if not success:
#                 # TODO. Send error.
#                 break


class NothingFile(MetroFile):
    pass


# TODO: Make sure that an optional file is not read because it exists (from a previous run), while
# it is not supposed to be here given the config.
# I.e., delete files which are not supposed to be generated.
def run_pipeline(
    config: Config,
    step_classes: list[type[Step]],
    dry_run: bool = False,
) -> None:
    metro_logger.setup()
    graph = nx.DiGraph()
    defined_steps = list()
    defined_output_files = set()
    all_output_files = set()
    for step_class in step_classes:
        # Instantiate the step with the config.
        step = step_class(config)
        all_output_files.update(step.output_files.values())
        if step.is_defined():
            defined_steps.append(step)
            # Add step to the graph.
            for ofile in step.output_files.values():
                defined_output_files.add(ofile)
                req_files = step.required_files()
                if req_files:
                    for ifile in req_files.values():
                        graph.add_edge(ifile, ofile, optional=False, step=step)
                else:
                    graph.add_edge(NothingFile, ofile, optional=False, step=step)
                for ifile in step.optional_files().values():
                    graph.add_edge(ifile, ofile, optional=True, step=step)
    to_delete_files = list()
    for ofile in all_output_files:
        f = ofile.from_dir(config.main_directory)
        if ofile not in defined_output_files and f.exists():
            to_delete_files.append(f)
    if to_delete_files:
        msg = "The following file(s) are not used anymore and will be removed:\n- "
        msg += "\n- ".join(map(lambda f: str(f.get_path()), to_delete_files))
        logger.warning(msg)
        if click.confirm("Continue?"):
            for f in to_delete_files:
                f.remove()
        else:
            sys.exit()
    outdated_steps = set()
    for step in defined_steps:
        if step.update_required():
            outdated_steps.add(step)
    degree_zero_nodes = {n for n, d in graph.in_degree(graph.nodes) if d == 0}
    required_edges = [(u, v) for u, v, data in graph.edges(data=True) if not data["optional"]]
    required_graph = graph.edge_subgraph(required_edges)
    feasible_files = {NothingFile}
    for f in required_graph.nodes:
        terminal_nodes = nx.ancestors(required_graph, f).intersection(degree_zero_nodes)
        if terminal_nodes == {NothingFile}:
            # File `f` can be generated by starting from "nothing" only.
            feasible_files.add(f)
    # Create a subgraph of all the files that can be generated starting from "nothing".
    subgraph = graph.subgraph(feasible_files)
    # Find all the files that need to be regenerated (a prerequisite step need to be re-run).
    # A file w needs to be regenerated if there is an edge (u, v) on any path from the origin node
    # to w
    endpoints = {n for n, d in graph.out_degree(graph.nodes) if d == 0}
    to_run_steps = set()
    for path in nx.all_simple_edge_paths(subgraph, NothingFile, endpoints):
        update_required = False
        for edge in path:
            step = subgraph.edges[edge]["step"]
            update_required |= step in outdated_steps
            if update_required:
                to_run_steps.add(step)
    # Compute a feasible order for file generation.
    file_order = list(nx.lexicographical_topological_sort(subgraph, key=lambda n: n.__name__))
    # Find a step order which generate the files in the computed order.
    all_steps = set()
    step_order = list()
    for f in file_order:
        for path in nx.all_simple_edge_paths(subgraph, NothingFile, f):
            for edge in path:
                step = subgraph.edges[edge]["step"]
                if step not in all_steps:
                    step_order.append(step)
                    all_steps.add(step)
    if dry_run:
        s = ""
        for i, step in enumerate(step_order):
            attrs = list()
            if step in to_run_steps:
                if step in outdated_steps:
                    attrs.append("bold")
                    color = "red"
                else:
                    color = "yellow"
            else:
                attrs.append("strike")
                color = "green"
            dep_str = colored(f"{i + 1}. {step}", color, attrs=attrs)
            s += dep_str + "\n"
        print(s)
        layers = {i: nodes for i, nodes in enumerate(nx.topological_generations(subgraph))}
        pos = nx.multipartite_layout(subgraph, subset_key=layers)
        fig, ax = plt.subplots(figsize=(10, 10))
        nx.draw_networkx(
            subgraph,
            pos=pos,
            with_labels=True,
            font_size=8,
            ax=ax,
            labels={n: n.__name__ for n in subgraph.nodes},
        )
        fig.savefig("tmp.png")
    else:
        n = len(to_run_steps)
        i = 1
        for step in step_order:
            if step not in to_run_steps:
                continue
            logger.info(f"=== Step {i} / {n}: {step} ===")
            step.execute(config)
            i += 1
