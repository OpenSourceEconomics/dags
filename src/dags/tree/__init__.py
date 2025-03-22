"""Module for handling DAG trees with nested dictionaries and qualified names."""

from dags.tree.dag_tree import (
    concatenate_functions_tree,
    create_dag_tree,
    create_input_structure_tree,
    functions_without_tree_logic,
    one_function_without_tree_logic,
)
from dags.tree.tree_utils import (
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
from dags.tree.validation import (
    fail_if_path_elements_have_trailing_undersores,
    fail_if_paths_are_invalid,
    fail_if_top_level_elements_repeated_in_paths,
    fail_if_top_level_elements_repeated_in_single_path,
)

__all__ = [
    # Primary functions
    "create_input_structure_tree",
    "create_dag_tree",
    "concatenate_functions_tree",
    "functions_without_tree_logic",
    "one_function_without_tree_logic",
    # Validation functions
    "fail_if_paths_are_invalid",
    "fail_if_path_elements_have_trailing_undersores",
    "fail_if_top_level_elements_repeated_in_paths",
    "fail_if_top_level_elements_repeated_in_single_path",
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
