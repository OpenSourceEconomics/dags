"""Tests for tree_utils module of dag_tree."""

from __future__ import annotations

import pytest

# Import fixtures
from dags.dag_tree.tree_utils import (
    _flatten_str_dict,
    _get_namespace_and_simple_name,
    _get_qualified_name,
    _is_python_identifier,
    _is_qualified_name,
    _unflatten_str_dict,
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


def test_flatten_str_dict(functions) -> None:
    from .conftest import (
        _global__f,
        _global__g,
        _namespace1__deep__f,
        _namespace1__f,
        _namespace1__f1,
        _namespace2__f,
        _namespace2__f2,
    )

    assert _flatten_str_dict(functions) == {
        "f": _global__f,
        "g": _global__g,
        "namespace1__f": _namespace1__f,
        "namespace1__f1": _namespace1__f1,
        "namespace1__deep__f": _namespace1__deep__f,
        "namespace2__f": _namespace2__f,
        "namespace2__f2": _namespace2__f2,
    }


def test_unflatten_str_dict(functions) -> None:
    from .conftest import (
        _global__f,
        _global__g,
        _namespace1__deep__f,
        _namespace1__f,
        _namespace1__f1,
        _namespace2__f,
        _namespace2__f2,
    )

    assert (
        _unflatten_str_dict(
            {
                "f": _global__f,
                "g": _global__g,
                "namespace1__f": _namespace1__f,
                "namespace1__f1": _namespace1__f1,
                "namespace1__deep__f": _namespace1__deep__f,
                "namespace2__f": _namespace2__f,
                "namespace2__f2": _namespace2__f2,
            },
        )
        == functions
    )


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
