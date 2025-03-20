"""Tests for parameters handling in dag_tree."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from dags.tree.dag_tree import (
    _get_parameter_absolute_path,
    _get_parameter_rel_to_abs_mapper,
    _get_parameter_tree_path,
    _get_top_level_namespace,
)

if TYPE_CHECKING:
    from dags.tree.typing import (
        GenericCallable,
        TreePathFunctionDict,
    )


def f():
    return None


def g(a, b, c):
    return a, b, c


def h(a, b__c):
    return a, b__c


@pytest.mark.parametrize(
    ("tree_path_functions", "top_level_inputs", "expected"),
    [
        ({"a": lambda a: a}, {"b"}, {"a", "b"}),
        ({"a": lambda a: a}, set(), {"a"}),
        ({"b": {"a": lambda a: a}}, set(), {"b"}),
        ({"b": {"a": lambda a: a}, "c": lambda c: c}, {"a"}, {"a", "b", "c"}),
    ],
)
def test_get_top_level_namespace(
    tree_path_functions: TreePathFunctionDict,
    top_level_inputs: set[str],
    expected: set[str],
) -> None:
    assert _get_top_level_namespace(tree_path_functions, top_level_inputs) == expected


@pytest.mark.parametrize(
    (
        "func",
        "current_namespace",
        "top_level_namespace",
        "all_qual_names",
        "expected",
    ),
    [
        (
            f,
            "",
            {"f"},
            {"f"},
            {},
        ),
        (
            f,
            ("n",),
            {"n", "a"},
            {"n__f", "a"},
            {},
        ),
        (
            g,
            (),
            {"g", "a", "b", "c"},
            {"g", "a", "b", "c"},
            {"a": "a", "b": "b", "c": "c"},
        ),
        (
            g,
            ("n",),
            {"n", "a"},
            {"n__g", "a", "n__b", "n__c"},
            {"a": "a", "b": "n__b", "c": "n__c"},
        ),
        (
            h,
            ("n",),
            {"n", "a"},
            {"n__h", "a", "n__b__c"},
            {"a": "a", "b__c": "n__b__c"},
        ),
        (
            h,
            ("n",),
            {"n"},
            {"n__h", "n__a", "n__b__c"},
            {"a": "n__a", "b__c": "n__b__c"},
        ),
    ],
)
def test_create_parameter_name_mapper(
    func: GenericCallable,
    current_namespace: tuple[str, ...],
    top_level_namespace: set[str],
    all_qual_names: set[str],
    expected: dict[str, str],
) -> None:
    assert (
        _get_parameter_rel_to_abs_mapper(
            func=func,
            current_namespace=current_namespace,
            top_level_namespace=top_level_namespace,
            all_qual_names=all_qual_names,
        )
        == expected
    )


def test_map_parameter_raises() -> None:
    with pytest.raises(ValueError, match="Cannot resolve parameter"):
        _get_parameter_absolute_path(
            parameter_name="x",
            current_namespace=(),
            top_level_namespace={"n"},
            all_qual_names={"n__y"},
        )


@pytest.mark.parametrize(
    ("parameter_name", "current_namespace", "top_level_namespace", "expected"),
    [
        ("x", (), set(), ("x",)),
        ("x", ("n",), {"n"}, ("n", "x")),
        ("x", ("n",), {"n", "x"}, ("x",)),
        ("n__x", ("n",), {"n"}, ("n", "x")),
    ],
)
def test_link_parameter_to_function_or_input(
    parameter_name: str,
    current_namespace: tuple[str, ...],
    top_level_namespace: set[str],
    expected: tuple[str, ...],
) -> None:
    assert (
        _get_parameter_tree_path(
            parameter_name=parameter_name,
            current_namespace=current_namespace,
            top_level_namespace=top_level_namespace,
        )
        == expected
    )
