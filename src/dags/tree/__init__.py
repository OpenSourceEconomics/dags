"""Module for handling DAG trees with nested dictionaries and qualified names."""

import warnings
from collections.abc import Callable
from typing import Any

from dags.tree.dag_tree import (
    concatenate_functions_tree,
    create_dag_tree,
    create_tree_with_input_types,
    get_functions_without_tree_logic,
    get_one_function_without_tree_logic,
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
from dags.tree.typing import NestedFunctionDict, QNameFunctionDict
from dags.tree.validation import (
    fail_if_paths_are_invalid as _fail_if_paths_are_invalid,
)


def one_function_without_tree_logic(
    function: Callable[..., Any],
    tree_path: tuple[str, ...],
    top_level_namespace: set[str],
) -> Callable[..., Any]:
    """Deprecated: Use get_one_function_without_tree_logic instead."""
    warnings.warn(
        "'one_function_without_tree_logic' is deprecated, "
        "update the package calling it to a version released after 15 March 2026.",
        FutureWarning,
        stacklevel=2,
    )
    return get_one_function_without_tree_logic(
        function=function,
        tree_path=tree_path,
        top_level_namespace=top_level_namespace,
    )


def fail_if_paths_are_invalid(
    *args: Any,
    **kwargs: Any,
) -> None:
    """Deprecated: Use dags.tree.validation.fail_if_paths_are_invalid instead."""
    warnings.warn(
        "'fail_if_paths_are_invalid' is deprecated, "
        "update the package calling it to a version released after 15 March 2026.",
        FutureWarning,
        stacklevel=2,
    )
    _fail_if_paths_are_invalid(*args, **kwargs)


def functions_without_tree_logic(
    functions: NestedFunctionDict,
    top_level_namespace: set[str],
) -> QNameFunctionDict:
    """Deprecated: Use get_functions_without_tree_logic instead."""
    warnings.warn(
        "'functions_without_tree_logic' is deprecated, "
        "update the package calling it to a version released after 15 March 2026.",
        FutureWarning,
        stacklevel=2,
    )
    return get_functions_without_tree_logic(
        functions=functions,
        top_level_namespace=top_level_namespace,
    )


__all__ = [
    # Primary functions
    "create_tree_with_input_types",
    "create_dag_tree",
    "concatenate_functions_tree",
    "get_functions_without_tree_logic",
    "get_one_function_without_tree_logic",
    # Deprecated
    "fail_if_paths_are_invalid",
    "functions_without_tree_logic",
    "one_function_without_tree_logic",
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
