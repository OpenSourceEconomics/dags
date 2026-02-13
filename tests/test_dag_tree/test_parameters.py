"""Tests for parameters handling in dag_tree."""

import inspect
from typing import Any

import pytest

from dags.tree.dag_tree import (
    _get_parameter_tree_path,
    _get_top_level_namespace_final,
    _get_top_level_namespace_initial,
    _map_parameters_rel_to_abs,
    functions_without_tree_logic,
)
from dags.tree.typing import (
    Callable,
    NestedFunctionDict,
    NestedInputStructureDict,
)


def f():
    return None


def g(a, b, c):
    return a, b, c


def h(a, b__c):
    return a, b__c


@pytest.mark.parametrize(
    ("functions", "top_level_inputs", "expected"),
    [
        ({"a": lambda a: a}, {"b"}, {"a", "b"}),
        ({"a": lambda a: a}, set(), {"a"}),
        ({"b": {"a": lambda a: a}}, set(), {"b"}),
        ({"b": {"a": lambda a: a}, "c": lambda c: c}, {"a"}, {"a", "b", "c"}),
    ],
)
def test_get_top_level_namespace_initial(
    functions: NestedFunctionDict,
    top_level_inputs: set[str],
    expected: set[str],
) -> None:
    assert (
        _get_top_level_namespace_initial(
            functions=functions,
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
    func: Callable[..., Any],
    current_namespace: tuple[str, ...],
    top_level_namespace: set[str],
    expected: dict[str, str],
) -> None:
    assert (
        _map_parameters_rel_to_abs(
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
        "qname_function_name_to_check",
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
    qname_function_name_to_check: str,
    expected_argument_name: str,
) -> None:
    top_level_namespace = _get_top_level_namespace_final(
        functions=functions,
        inputs=input_structure,
    )
    qname_functions = functions_without_tree_logic(
        functions=functions,
        top_level_namespace=top_level_namespace,
    )
    assert (
        expected_argument_name
        in inspect.signature(qname_functions[qname_function_name_to_check]).parameters
    )
