from pathlib import Path

from treelib import Tree
from treelib.exceptions import DuplicatedNodeIdError

from pymetropolis.schema import FILES


class Node:
    def __init__(self, display: str):
        self.display = display


def expand_dir(children, names: dict[str, str], root: bool = False):
    s = ""
    for node in children:
        if isinstance(node, str):
            # This is a file.
            name = names[node].lower()
            s += f'<li><a href="#{name}">{node}</a></li>\n'
        elif isinstance(node, dict):
            # This is a directory.
            assert len(node) == 1
            dirname = list(node.keys())[0]
            assert "children" in node[dirname]
            if not root:
                s += "<li>\n"
            s += f"<details open>\n<summary><b>{dirname}</b></summary>\n<ul>\n"
            s += expand_dir(node[dirname]["children"], names)
            s += "</ul>\n</details>\n"
            if not root:
                s += "</li>\n"
    return s


if __name__ == "__main__":
    tree = Tree()
    tree.create_node(tag="main_directory", identifier="root", data=Node("main_directory"))
    file_map = dict()
    file_names = dict()

    for f in FILES:
        path = Path(f.path)
        identifier = "root"
        parent = "root"
        for part in path.parts:
            identifier += f".{part}"
            try:
                tree.create_node(
                    tag=part,
                    identifier=identifier,
                    parent=parent,
                    data=Node(f"{part} (HELLO)"),
                )
            except DuplicatedNodeIdError:
                pass
            parent = identifier
        file_map[identifier] = f
        file_names[part] = f.__name__

    # tree_str = tree.show(stdout=False, data_property="display")

    doc = "# MetroFiles\n\n"

    # Add tree directory.
    tree_dict = tree.to_dict(sort=True)
    tree_str = '<div class="tree">\n'
    tree_str += expand_dir([tree_dict], file_names, root=True)
    tree_str += "</div>\n\n"
    doc += tree_str

    for identifier in tree.expand_tree():
        if identifier not in file_map.keys():
            # This is a directory.
            continue
        f = file_map[identifier]
        doc += f._md_doc(simple=False)
        doc += "\n"
    print(doc)
