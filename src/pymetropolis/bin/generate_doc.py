import argparse
import os
from collections import defaultdict
from pathlib import Path

from treelib import Tree
from treelib.exceptions import DuplicatedNodeIdError

from pymetropolis.schema import FILES, STEPS


def expand_dir(children, root: bool = False):
    s = ""
    for node in children:
        assert isinstance(node, dict)
        assert len(node) == 1
        name, value = next(iter(node.items()))
        if "children" in value:
            # This is a directory.
            assert "children" in value
            if not root:
                s += "<li>\n"
            s += f"<details open>\n<summary><b>{name}</b></summary>\n<ul>\n"
            s += expand_dir(value["children"])
            s += "</ul>\n</details>\n"
            if not root:
                s += "</li>\n"
        else:
            # This is a file.
            link = value["data"]["name"].lower()
            s += f'<li><a href="#{link}">{name}</a></li>\n'
    return s


def build_files_doc() -> str:
    # Build a directory tree of all the files.
    tree = Tree()
    tree.create_node(tag="main_directory", identifier="root")
    file_map = dict()

    for f in FILES:
        path = Path(f.path)
        identifier = "root"
        parent = "root"
        for part in path.parts:
            identifier += f".{part}"
            try:
                tree.create_node(
                    tag=part, identifier=identifier, parent=parent, data={"name": f.__name__}
                )
            except DuplicatedNodeIdError:
                pass
            parent = identifier
        file_map[identifier] = f

    files_steps = defaultdict(lambda: list())
    # Iterate over the steps to collect the generated files.
    for step in sorted(STEPS, key=lambda s: s.__name__):
        for ofile in step.output_files.values():
            files_steps[ofile].append(step.__name__)

    doc = ""
    # Add tree directory.
    tree_dict = tree.to_dict(sort=True, with_data=True)
    tree_str = '<div class="tree">\n'
    tree_str += expand_dir([tree_dict], root=True)
    tree_str += "</div>\n\n"
    doc += tree_str
    for identifier in tree.expand_tree():
        if identifier not in file_map.keys():
            # This is a directory.
            continue
        f = file_map[identifier]
        doc += f._md_doc()
        steps = files_steps[f]
        if steps:
            doc += (
                "- **Steps:** "
                + ", ".join(map(lambda s: f"[`{s}`](steps.html#{s.lower()})", steps))
                + "\n"
            )
        doc += f._md_doc_schema(simple=False)
        doc += "\n"
    return doc


def build_steps_doc() -> str:
    doc = ""
    for step in sorted(STEPS, key=lambda s: s.__name__):
        doc += step._md_doc()
        doc += "\n"
    return doc


def build_params_doc() -> str:
    doc = ""
    all_params = dict()
    params_steps = defaultdict(lambda: list())
    # Iterate over the steps to collect the parameters.
    for step in sorted(STEPS, key=lambda s: s.__name__):
        for _, param_obj in step._iter_params():
            key_str = ".".join(param_obj.key)
            all_params[key_str] = param_obj
            params_steps[key_str].append(step.__name__)
    # Iterate over the parameters, considering first the single-key parameters, then the table,
    #  parameters, sorted by alphabetical order.
    for param_name, param_obj in sorted(
        all_params.items(), key=lambda i: (len(i[1].key) != 1, i[1].key)
    ):
        doc += param_obj._md_doc()
        doc += "- **Steps:** " + ", ".join(
            map(lambda s: f"[`{s}`](steps.html#{s.lower()})", params_steps[param_name])
        )
        doc += "\n\n"
    return doc


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Write reference documentation")
    parser.add_argument("path", type=str, help="Path to the directory to store the markdown files")
    args = parser.parse_args()

    if not os.path.isdir(args.path):
        os.makedirs(args.path)

    print("Generating MetroFiles references")
    files_doc = build_files_doc()
    with open(os.path.join(args.path, "files-generated.md"), "w") as f:
        f.write(files_doc)

    print("Generating Steps references")
    steps_doc = build_steps_doc()
    with open(os.path.join(args.path, "steps-generated.md"), "w") as f:
        f.write(steps_doc)

    print("Generating Parameters references")
    params_doc = build_params_doc()
    with open(os.path.join(args.path, "parameters-generated.md"), "w") as f:
        f.write(params_doc)
