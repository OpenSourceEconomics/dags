"""Utilities for handling qualified names in nested dictionaries."""

import flatten_dict as fd
import flatten_dict.reducers
import flatten_dict.splitters

from dags.tree.typing import FlatQNameDict, FlatTreePathDict, NestedStructureDict

# Constants for qualified names
QNAME_DELIMITER: str = "__"

# Reducers and splitters to flatten/unflatten dicts with qualified names as keys
_qualified_name_reducer = fd.reducers.make_reducer(delimiter=QNAME_DELIMITER)
_qualified_name_splitter = fd.splitters.make_splitter(delimiter=QNAME_DELIMITER)


def qname_from_tree_path(tree_path: tuple[str, ...]) -> str:
    """Convert a tree path to a qualified name.

    Args:
        tree_path: A tuple of strings.

    Returns:
    -------
        A qualified name.
    """
    return QNAME_DELIMITER.join(tree_path)


def tree_path_from_qname(qname: str) -> tuple[str, ...]:
    """Convert a qualified name to a tree path (tuple of strings).

    Args:
        qname: A qualified name.

    Returns:
    -------
        A tree path.
    """
    return tuple(qname.split(QNAME_DELIMITER))


def flatten_to_qnames(nested: NestedStructureDict) -> FlatQNameDict:
    """Flatten a nested dictionary to a flat dictionary with qualified names as keys.

    Args:
        nested: A nested dictionary.

    Returns:
    -------
        A flat dictionary with qualified names as keys.
    """
    return fd.flatten(nested, reducer=_qualified_name_reducer)


def qnames(nested: NestedStructureDict) -> list[str]:
    """Return a list of qualified names from the keys of the nested dictionary.

    Args:
        nested: A nested dictionary.

    Returns:
    -------
        A list of qualified names.
    """
    return list(flatten_to_qnames(nested).keys())


def unflatten_from_qnames(flat_qnames: FlatQNameDict) -> NestedStructureDict:
    """Return a nested dictionary from a flat dictionary with qualified names as keys.

    Args:
        flat_qnames: A dictionary with qualified names as keys.

    Returns:
    -------
        A nested dictionary.
    """
    return fd.unflatten(flat_qnames, splitter=_qualified_name_splitter)


def flatten_to_tree_paths(nested: NestedStructureDict) -> FlatTreePathDict:
    """Flatten a nested dictionary to a flat dictionary with tree paths as keys.

    Args:
        nested: A nested dictionary.

    Returns:
    -------
        A flat dictionary with qualified names as keys.
    """
    return fd.flatten(nested, reducer="tuple")


def tree_paths(nested: NestedStructureDict) -> list[tuple[str, ...]]:
    """Return a list of tree paths of the nested dictionary.

    Args:
        nested: A nested dictionary.

    Returns:
    -------
        A list of tuples.
    """
    return list(flatten_to_tree_paths(nested).keys())


def unflatten_from_tree_paths(flat_tree_paths: FlatTreePathDict) -> NestedStructureDict:
    """Return a nested dictionary from a flat dictionary with tree paths as keys.

    Args:
        flat_tree_paths: A flat dictionary with tree paths (tuples) as keys.

    Returns:
    -------
        A nested dictionary.
    """
    return fd.unflatten(flat_tree_paths, splitter="tuple")
