"""Tests for parameters handling in dag_tree."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

import pytest

from dags.tree.dag_tree import (
    _get_parameter_rel_to_abs_mapper,
    _get_parameter_tree_path,
    _get_top_level_namespace_final,
    _get_top_level_namespace_initial,
    functions_for_dags_concatenate_functions,
)

if TYPE_CHECKING:
    from dags.tree.typing import (
        GenericCallable,
        NestedFunctionDict,
        NestedInputStructureDict,
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
def test_get_top_level_namespace_initial(
    tree_path_functions: TreePathFunctionDict,
    top_level_inputs: set[str],
    expected: set[str],
) -> None:
    assert (
        _get_top_level_namespace_initial(
            tree_paths=set(tree_path_functions.keys()),
            top_level_inputs=top_level_inputs,
        )
        == expected
    )


@pytest.mark.parametrize(
    (
        "func",
        "current_namespace",
        "top_level_namespace",
        "expected",
    ),
    [
        (
            f,
            "",
            {"f"},
            {},
        ),
        (
            f,
            ("n",),
            {"n", "a"},
            {},
        ),
        (
            g,
            (),
            {"g", "a", "b", "c"},
            {"a": "a", "b": "b", "c": "c"},
        ),
        (
            g,
            ("n",),
            {"n", "a"},
            {"a": "a", "b": "n__b", "c": "n__c"},
        ),
        (
            h,
            ("n",),
            {"n", "a"},
            {"a": "a", "b__c": "n__b__c"},
        ),
        (
            h,
            ("n",),
            {"n"},
            {"a": "n__a", "b__c": "n__b__c"},
        ),
    ],
)
def test_create_parameter_name_mapper(
    func: GenericCallable,
    current_namespace: tuple[str, ...],
    top_level_namespace: set[str],
    expected: dict[str, str],
) -> None:
    assert (
        _get_parameter_rel_to_abs_mapper(
            func=func,
            current_namespace=current_namespace,
            top_level_namespace=top_level_namespace,
        )
        == expected
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


@pytest.mark.parametrize(
    (
        "functions",
        "input_structure",
        "qual_name_function_name_to_check",
        "expected_argument_name",
    ),
    [
        (
            {
                "top_level": {"foo": lambda x: x},
                "target_namespace": {
                    "nested_level": {"foo": lambda x: x},
                    "target_leaf": lambda top_level__foo: top_level__foo,
                },
            },
            {
                "top_level": {"x": None},
                "target_namespace": {"nested_level": {"x": None}},
            },
            "target_namespace__target_leaf",
            "top_level__foo",
        ),
        (
            {
                "top_level": {"föö": lambda x: x},
                "target_namespace": {
                    "nested_level": {"föö": lambda x: x},
                    "target_leaf": lambda top_level__föö: top_level__föö,
                },
            },
            {
                "top_level": {"x": None},
                "target_namespace": {"nested_level": {"x": None}},
            },
            "target_namespace__target_leaf",
            "top_level__föö",
        ),
    ],
)
def test_correct_argument_names(
    functions: NestedFunctionDict,
    input_structure: NestedInputStructureDict,
    qual_name_function_name_to_check: str,
    expected_argument_name: str,
) -> None:
    top_level_namespace = _get_top_level_namespace_final(
        functions=functions,
        input_structure=input_structure,
    )

    qual_name_functions = functions_for_dags_concatenate_functions(
        functions=functions,
        input_structure=input_structure,
        top_level_namespace=top_level_namespace,
        perform_checks=True,
    )
    assert (
        expected_argument_name
        in inspect.signature(
            qual_name_functions[qual_name_function_name_to_check]
        ).parameters
    )
