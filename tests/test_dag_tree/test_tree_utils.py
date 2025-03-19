"""Tests for tree_utils module of dag_tree."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from dags.dag_tree.typing import NestedFunctionDict

# Import fixtures
from dags.dag_tree.tree_utils import (
    _get_namespace_and_simple_name,
    _get_qualified_name,
    _is_python_identifier,
    _is_qualified_name,
    flatten_to_qual_names,
    flatten_to_tree_paths,
    qual_name_from_tree_path,
    qual_names,
    tree_path_from_qual_name,
    tree_paths,
    unflatten_from_qual_names,
    unflatten_from_tree_paths,
)


@pytest.mark.parametrize(
    ("qualified_name", "expected"),
    [
        ("", ("", "")),
        ("a", ("", "a")),
        ("a__b", ("a", "b")),
        ("a___b", ("a", "_b")),
    ],
)
def test_get_namespace_and_simple_name(
    qualified_name: str, expected: tuple[str, str]
) -> None:
    assert _get_namespace_and_simple_name(qualified_name) == expected


@pytest.mark.parametrize(
    ("namespace", "simple_name", "expected"),
    [
        ("", "", ""),
        ("", "a", "a"),
        ("a", "b", "a__b"),
    ],
)
def test_get_qualified_name(namespace: str, simple_name: str, expected: str) -> None:
    assert _get_qualified_name(namespace, simple_name) == expected


@pytest.mark.parametrize(
    ("s", "expected"),
    [
        ("", False),
        ("1", False),
        ("_", True),
        ("_1", True),
        ("__", True),
        ("_a", True),
        ("_A", True),
        ("a", True),
        ("a1", True),
        ("a_", True),
        ("ab", True),
        ("aB", True),
        ("A", True),
        ("A1", True),
        ("A_", True),
        ("Ab", True),
        ("AB", True),
        ("ÄB", True),
        ("ä", True),
        ("äb", True),
        ("Ä", True),
        ("ß", True),
    ],
)
def test_is_python_identifier(s: str, expected: bool) -> None:
    assert _is_python_identifier(s) == expected


@pytest.mark.parametrize(
    ("s", "expected"),
    [
        ("a", False),
        ("__", False),
        ("a__", False),
        ("__a", False),
        ("a__b", True),
    ],
)
def test_is_qualified_name(s: str, expected: bool) -> None:
    assert _is_qualified_name(s) == expected


def _bb() -> None:
    return None


def _ee() -> None:
    return None


def _ff() -> None:
    return None


@pytest.fixture
def functions_tree() -> NestedFunctionDict:
    return {
        "a": {"b": _bb},
        "c": {"d": {"e": _ee}},
        "f": _ff,
    }


def test_flatten_to_qual_names(functions_tree: NestedFunctionDict) -> None:
    assert flatten_to_qual_names(functions_tree) == {
        "a__b": _bb,
        "c__d__e": _ee,
        "f": _ff,
    }


def test_round_trip_via_qual_names(functions_tree: NestedFunctionDict) -> None:
    assert (
        unflatten_from_qual_names(flatten_to_qual_names(functions_tree))
        == functions_tree
    )


def test_qual_names(functions_tree: NestedFunctionDict) -> None:
    assert qual_names(functions_tree) == [
        "a__b",
        "c__d__e",
        "f",
    ]


def test_flatten_to_tree_paths(functions_tree: NestedFunctionDict) -> None:
    assert flatten_to_tree_paths(functions_tree) == {
        ("a", "b"): _bb,
        ("c", "d", "e"): _ee,
        ("f",): _ff,
    }


def test_round_trip_via_tree_paths(functions_tree: NestedFunctionDict) -> None:
    assert (
        unflatten_from_tree_paths(flatten_to_tree_paths(functions_tree))
        == functions_tree
    )


def test_tree_paths(functions_tree: NestedFunctionDict) -> None:
    assert tree_paths(functions_tree) == [
        ("a", "b"),
        ("c", "d", "e"),
        ("f",),
    ]


def test_qual_name_from_tree_path() -> None:
    assert qual_name_from_tree_path(("a", "b")) == "a__b"


def test_tree_path_from_qual_name() -> None:
    assert tree_path_from_qual_name("a__b") == ("a", "b")
