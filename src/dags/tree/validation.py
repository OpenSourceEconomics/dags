"""Validation utilities for DAG trees."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dags.dag import format_list_linewise
from dags.tree.exceptions import (
    RepeatedTopLevelElementError,
    TrailingUnderscoreError,
)
from dags.tree.tree_utils import tree_path_from_qname, tree_paths

if TYPE_CHECKING:
    from dags.tree.typing import (
        NestedFunctionDict,
        NestedInputDict,
        NestedStructureDict,
        NestedTargetDict,
        QNameFunctionDict,
    )


def fail_if_paths_are_invalid(
    functions: NestedFunctionDict | None = None,
    abs_qnames_functions: QNameFunctionDict | None = None,
    data_tree: NestedStructureDict | None = None,
    input_structure: NestedInputDict | None = None,
    targets: NestedTargetDict | None = None,
    top_level_namespace: set[str] | list[str] | tuple[str, ...] = (),
) -> None:
    """
    Fail if the paths in the (different parts of the) functions tree are invalid.

    The interface is designed so you can pass any argument you like, but none of them is
    required (however, not passing anything does not make sense).

    There are two reasons for failure:

    1. Path elements have trailing underscores.
    2. The paths contain elements that are part of the top-level namespace.

    Note: Sometimes you want to pass both `functions` (the nested function dict you will
    start out with) and `abs_qnames_functions` (the result of running
    `functions_without_tree_logic` on `functions`, which contains the converted
    parameters of functions, too). Even though the former may be seen as a subset of the
    latter, the conversion to qualified absolute names is not innocuous when it comes to
    the check for trailing underscores. The reason is that the conversion from qualified
    names to tree paths assigns any third consecutive underscore to the name that comes
    after the double underscore separating two levels of nesting.

    Args:
        functions:
            The nested function dict.
        abs_qnames_functions:
            The result of running `functions_without_tree_logic` on `functions`.
        data_tree:
            The tree of input data (typically not used together with `input_structure`).
        input_structure:
            The structure of inputs (typically not used together with `data_tree`).
        targets:
            The tree of targets to be computed.
        top_level_namespace:
            The set of top-level namespace elements (required for the check regarding
            repetition of elements).


    Raises
    ------
        TrailingUnderscoreError: If the paths in the functions tree are invalid.
        RepeatedTopLevelElementError: If the paths in the functions tree are invalid.
    """
    if functions is None:
        functions = {}
    if abs_qnames_functions is None:
        abs_qnames_functions = {}
    if data_tree is None:
        data_tree = {}
    if input_structure is None:
        input_structure = {}
    if targets is None:
        targets = {}
    all_tree_paths = (
        set(tree_paths(functions))
        | {tree_path_from_qname(qn) for qn in abs_qnames_functions}
        | set(tree_paths(data_tree))
        | set(tree_paths(input_structure))
        | set(tree_paths(targets))
    )
    fail_if_path_elements_have_trailing_undersores(all_tree_paths)
    fail_if_top_level_elements_repeated_in_paths(
        all_tree_paths=all_tree_paths,
        top_level_namespace=set(top_level_namespace),
    )


def fail_if_path_elements_have_trailing_undersores(
    all_tree_paths: set[tuple[str, ...]],
) -> None:
    """
    Check if any element of the tree path except for the leaf ends with an underscore.

    Args:
        tree_paths:
            The tree paths.

    Raises
    ------
        TrailingUnderscoreError: If any branch of the functions tree ends with an
            underscore.
    """
    collected_errors = {
        path
        for path in all_tree_paths
        if len(path) > 1 and any(name.endswith("_") for name in path[:-1])
    }
    if collected_errors:
        paths = "\n".join(str(p) for p in collected_errors)
        msg = (
            "Except for the leaf name, elements of the paths in the functions tree "
            f"must not end with an underscore. Offending path(s):\n\n{paths}"
        )
        raise TrailingUnderscoreError(msg)


def fail_if_top_level_elements_repeated_in_paths(
    all_tree_paths: set[tuple[str, ...]],
    top_level_namespace: set[str],
) -> None:
    """
    Fail if any element of the top-level namespace is repeated elsewhere.

    Args:
        all_tree_paths:
            All tree paths that are to be checked.
        top_level_namespace:
            The elements of the top-level namespace.

    Raises
    ------
        RepeatedTopLevelElementError: If any element of the top-level namespace is
            repeated further down in the hierarchy.
    """
    collected_errors = {
        path
        for path in all_tree_paths
        if len(path) > 1 and any((name in top_level_namespace) for name in path[1:])
    }
    if collected_errors:
        paths = "\n".join(str(p) for p in collected_errors)
        msg = (
            "Elements of the top-level namespace must not be repeated further down "
            f"in the hierarchy. Offending path(s):\n\n{paths}\n\n\n"
            "Top-level namespace:\n\n"
            f"{format_list_linewise(sorted(top_level_namespace))}"
        )
        raise RepeatedTopLevelElementError(msg)


def fail_if_top_level_elements_repeated_in_single_path(
    tree_path: tuple[str, ...],
    top_level_namespace: set[str],
) -> None:
    """Fail if elements of `tree_path` repeat elements of the top-level namespace.

    Args:
        top_level_namespace:
            The elements of the top-level namespace.
        tree_path:
            A single tree path.

    Raises
    ------
        RepeatedTopLevelElementError: If any element of `tree_path` equals an element in
            the top-level namespace.
    """
    if len(tree_path) > 1 and any(
        (name in top_level_namespace) for name in tree_path[1:]
    ):
        msg = (
            "Elements of the top-level namespace must not be repeated further down "
            f"in the hierarchy. Path:\n\n{tree_path}\n\n\n"
            f"Top-level namespace:\n\n{top_level_namespace}"
        )
        raise RepeatedTopLevelElementError(msg)
