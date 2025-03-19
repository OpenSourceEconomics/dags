"""Module for handling DAG trees with nested dictionaries and qualified names."""

from dags.dag_tree.dag_tree import (
    concatenate_functions_tree,
    create_dag_tree,
    create_input_structure_tree,
)
from dags.dag_tree.tree_utils import (
    QUAL_NAME_DELIMITER,
    flatten_to_qual_names,
    flatten_to_tree_paths,
    qual_name_from_tree_path,
    qual_names,
    tree_path_from_qual_name,
    tree_paths,
    unflatten_from_qual_names,
    unflatten_from_tree_paths,
)

__all__ = [
    # Primary functions
    "create_input_structure_tree",
    "create_dag_tree",
    "concatenate_functions_tree",
    # Qualified name utilities
    "QUAL_NAME_DELIMITER",
    "flatten_to_qual_names",
    "flatten_to_tree_paths",
    "qual_name_from_tree_path",
    "qual_names",
    "tree_path_from_qual_name",
    "tree_paths",
    "unflatten_from_qual_names",
    "unflatten_from_tree_paths",
]
