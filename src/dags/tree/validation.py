"""Validation utilities for DAG trees."""

from __future__ import annotations

import warnings
from itertools import combinations, groupby
from operator import itemgetter
from typing import TYPE_CHECKING, Literal

from dags.tree.tree_utils import (
    _get_namespace_and_simple_name,
    _get_qualified_name,
)

if TYPE_CHECKING:
    from dags.tree.typing import (
        QualNameFunctionDict,
        QualNameInputStructureDict,
    )


def _check_for_parent_child_name_clashes(
    qual_name_functions: QualNameFunctionDict,
    qual_name_input_structure: QualNameInputStructureDict,
    name_clashes_resolution: Literal["raise", "warn", "ignore"],
) -> None:
    """Raise an error if name clashes exist between parent and child functions/inputs.

    Args:
        qual_name_functions: A flat dictionary of functions.
        qual_name_input_structure: A flat dictionary of the input structure.
        name_clashes_resolution: Strategy for resolving name clashes.
    """
    if name_clashes_resolution == "ignore":
        return

    name_clashes: list[tuple[str, str]] = _find_parent_child_name_clashes(
        qual_name_functions, qual_name_input_structure
    )

    if len(name_clashes) > 0:
        message: str = f"There are name clashes: {name_clashes}."
        if name_clashes_resolution == "raise":
            raise ValueError(message)
        if name_clashes_resolution == "warn":
            warnings.warn(message, stacklevel=2)


def _find_parent_child_name_clashes(
    qual_name_functions: QualNameFunctionDict,
    qual_name_input_structure: QualNameInputStructureDict,
) -> list[tuple[str, str]]:
    """Find and return a list of tuples representing parent-child name clashes.

    Args:
        qual_name_functions: A flat dictionary of functions.
        qual_name_input_structure: A flat dictionary of the input structure.

    Returns
    -------
        A list of tuples, each containing two qualified names that clash.
    """
    _qualified_names: set[str] = set(qual_name_functions.keys()) | set(
        qual_name_input_structure.keys()
    )
    namespace_and_simple_names: list[tuple[str, str]] = [
        _get_namespace_and_simple_name(qname) for qname in _qualified_names
    ]
    # Group by simple name (only functions/inputs with the same simple name can clash)
    namespace_and_simple_names.sort(key=itemgetter(1))
    grouped_by_simple_name = groupby(namespace_and_simple_names, key=itemgetter(1))
    result: list[tuple[str, str]] = []
    for _, group in grouped_by_simple_name:
        group_list = list(group)
        for pair in combinations(group_list, 2):
            namespace_1, simple_name_1 = pair[0]
            namespace_2, simple_name_2 = pair[1]
            if namespace_1.startswith(namespace_2) or namespace_2.startswith(
                namespace_1
            ):
                result.append(
                    (
                        _get_qualified_name(namespace_1, simple_name_1),
                        _get_qualified_name(namespace_2, simple_name_2),
                    )
                )
    return result


def fail_if_path_elements_have_trailing_undersores(
    tree_paths: set[tuple[str, ...]],
) -> None:
    """
    Check if any element of the tree path except for the leaf ends with an underscore.

    Args:
        tree_paths:
            The tree paths.

    Raises
    ------
        ValueError: If any branch of the functions tree ends with an underscore.
    """
    collected_errors = {
        path
        for path in tree_paths
        if len(path) > 1 and any(name.endswith("_") for name in path[:-1])
    }
    if collected_errors:
        paths = "\n".join(str(p) for p in collected_errors)
        msg = (
            "Except for the leaf name, elements of the paths in the functions tree "
            f"must not end with an underscore. Offending path(s):\n\n{paths}"
        )
        raise ValueError(msg)


def fail_if_top_level_elements_repeated_in_paths(
    top_level_namespace: set[str],
    tree_paths: set[tuple[str, ...]],
) -> None:
    """
    Check if any element of the tree paths except for the leaf ends with an underscore.

    Args:
        top_level_namespace:
            The elements of the top-level namespace.
        tree_paths:
            The tree paths.

    Raises
    ------
        ValueError: If any branch of the functions tree ends with an underscore.
    """
    collected_errors = {
        path
        for path in tree_paths
        if len(path) > 1 and any((name in top_level_namespace) for name in path[1:])
    }
    if collected_errors:
        paths = "\n".join(str(p) for p in collected_errors)
        msg = (
            "Elements of the top-level namespace must not be repeated further down "
            f"in the hierarchy. Offending path(s):\n\n{paths}\n\n\n"
            f"Top-level namespace:\n\n{top_level_namespace}"
        )
        raise ValueError(msg)


def fail_if_top_level_elements_repeated_in_single_path(
    top_level_namespace: set[str],
    tree_path: tuple[str, ...],
) -> None:
    """
    Check if any element of the tree path except for the leaf ends with an underscore.

    Args:
        top_level_namespace:
            The elements of the top-level namespace.
        tree_path:
            A single tree path.

    Raises
    ------
        ValueError: If any branch of the functions tree ends with an underscore.
    """
    if len(tree_path) > 1 and any(
        (name in top_level_namespace) for name in tree_path[1:]
    ):
        msg = (
            "Elements of the top-level namespace must not be repeated further down "
            f"in the hierarchy. Path:\n\n{tree_path}\n\n\n"
            f"Top-level namespace:\n\n{top_level_namespace}"
        )
        raise ValueError(msg)
