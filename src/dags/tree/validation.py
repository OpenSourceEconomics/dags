"""Validation utilities for DAG trees."""

from __future__ import annotations


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
    Fail if any element of the top-level namespace is repeated elsewhere.

    Args:
        top_level_namespace:
            The elements of the top-level namespace.
        tree_paths:
            The tree paths.

    Raises
    ------
        ValueError: If any element of the top-level namespace is repeated further down
            in the hierarchy.
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
    """Fail if elements of `tree_path` repeat elements of the top-level namespace.

    Args:
        top_level_namespace:
            The elements of the top-level namespace.
        tree_path:
            A single tree path.

    Raises
    ------
        ValueError: If any element of `tree_path` equals an element in the top-level
            namespace.
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
