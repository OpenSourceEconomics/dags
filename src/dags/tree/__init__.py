"""Module for handling DAG trees with nested dictionaries and qualified names."""

from __future__ import annotations

from dags.tree.dag_tree import (
    concatenate_functions_tree,
    create_dag_tree,
    create_tree_with_input_types,
    functions_without_tree_logic,
    one_function_without_tree_logic,
)
from dags.tree.exceptions import (
    RepeatedTopLevelElementError,
    TrailingUnderscoreError,
)
from dags.tree.tree_utils import (
    QNAME_DELIMITER,
    flatten_to_qnames,
    flatten_to_tree_paths,
    qname_from_tree_path,
    qnames,
    tree_path_from_qname,
    tree_paths,
    unflatten_from_qnames,
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
    "create_tree_with_input_types",
    "create_dag_tree",
    "concatenate_functions_tree",
    "functions_without_tree_logic",
    "one_function_without_tree_logic",
    # Validation functions
    "fail_if_paths_are_invalid",
    "fail_if_path_elements_have_trailing_undersores",
    "fail_if_top_level_elements_repeated_in_paths",
    "fail_if_top_level_elements_repeated_in_single_path",
    # Exceptions
    "RepeatedTopLevelElementError",
    "TrailingUnderscoreError",
    # Qualified name utilities
    "QNAME_DELIMITER",
    "flatten_to_qnames",
    "flatten_to_tree_paths",
    "qname_from_tree_path",
    "qnames",
    "tree_path_from_qname",
    "tree_paths",
    "unflatten_from_qnames",
    "unflatten_from_tree_paths",
]
