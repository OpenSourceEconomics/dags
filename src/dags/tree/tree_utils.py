"""Utilities for handling qualified names in nested dictionaries."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import flatten_dict as fd

if TYPE_CHECKING:
    from dags.tree.typing import FlatQualNameDict, FlatTreePathDict, NestedStructureDict

# Constants for qualified names
QUAL_NAME_DELIMITER: str = "__"
_python_identifier: str = r"[a-zA-Z_\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF][a-zA-Z0-9_\u00A0-\uD7FF\uF900-\uFDCF\uFDF0-\uFFEF]*"  # noqa: E501

# Reducers and splitters to flatten/unflatten dicts with qualified names as keys
_qualified_name_reducer = fd.reducers.make_reducer(delimiter=QUAL_NAME_DELIMITER)
_qualified_name_splitter = fd.splitters.make_splitter(delimiter=QUAL_NAME_DELIMITER)


def qual_name_from_tree_path(tree_path: tuple[str, ...]) -> str:
    """Convert a tree path to a qualified name.

    Args:
        tree_path: A tuple of strings.

    Returns
    -------
        A qualified name.
    """
    return QUAL_NAME_DELIMITER.join(tree_path)


def tree_path_from_qual_name(qual_name: str) -> tuple[str, ...]:
    """Convert a qualified name to a tree path (tuple of strings).

    Args:
        qual_name: A qualified name.

    Returns
    -------
        A tree path.
    """
    return tuple(qual_name.split(QUAL_NAME_DELIMITER))


def flatten_to_qual_names(nested: NestedStructureDict) -> FlatQualNameDict:
    """Flatten a nested dictionary to a flat dictionary with qualified names as keys.

    Args:
        nested: A nested dictionary.

    Returns
    -------
        A flat dictionary with qualified names as keys.
    """
    return fd.flatten(nested, reducer=_qualified_name_reducer)


def qual_names(nested: NestedStructureDict) -> list[str]:
    """Return a list of qualified names from the keys of the nested dictionary.

    Args:
        nested: A nested dictionary.

    Returns
    -------
        A list of qualified names.
    """
    return list(flatten_to_qual_names(nested).keys())


def unflatten_from_qual_names(flat_qual_names: FlatQualNameDict) -> NestedStructureDict:
    """Return a nested dictionary from a flat dictionary with qualified names as keys.

    Args:
        flat_qual_names: A dictionary with qualified names as keys.

    Returns
    -------
        A nested dictionary.
    """
    return fd.unflatten(flat_qual_names, splitter=_qualified_name_splitter)


def flatten_to_tree_paths(nested: NestedStructureDict) -> FlatTreePathDict:
    """Flatten a nested dictionary to a flat dictionary with tree paths as keys.

    Args:
        nested: A nested dictionary.

    Returns
    -------
        A flat dictionary with qualified names as keys.
    """
    return fd.flatten(nested, reducer="tuple")


def tree_paths(nested: NestedStructureDict) -> list[tuple[str, ...]]:
    """Return a list of tree paths of the nested dictionary.

    Args:
        nested: A nested dictionary.

    Returns
    -------
        A list of tuples.
    """
    return list(flatten_to_tree_paths(nested).keys())


def unflatten_from_tree_paths(flat_tree_paths: FlatTreePathDict) -> NestedStructureDict:
    """Return a nested dictionary from a flat dictionary with tree paths as keys.

    Args:
        flat_tree_paths: A flat dictionary with tree paths (tuples) as keys.

    Returns
    -------
        A nested dictionary.
    """
    return fd.unflatten(flat_tree_paths, splitter="tuple")


def _is_python_identifier(s: str) -> bool:
    """Check if a string is a valid Python identifier.

    Args:
        s: String to check

    Returns
    -------
        True if valid identifier, False otherwise
    """
    return bool(re.fullmatch(_python_identifier, s))
