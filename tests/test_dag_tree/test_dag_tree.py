"""Tests for the dag_tree module."""

import functools

import pytest

from dags.tree import concatenate_functions_tree
from dags.tree.typing import (
    NestedFunctionDict,
    NestedInputDict,
    NestedOutputDict,
    NestedTargetDict,
)


def f(g: int, a: int, b: float) -> float:
    return g + a + b


def g(a: int) -> int:
    return a**2


def h(a: int, b__g: int) -> int:
    return a + b__g


@pytest.fixture
def functions_simple() -> NestedFunctionDict:
    return {
        "n1": {
            "f": f,
            "g": g,
        },
        "n2": {
            "h": h,
        },
    }


@pytest.fixture
def functions_nested_and_duplicate_g() -> NestedFunctionDict:
    return {
        "n1": {
            "f": f,
            "g": g,
        },
        "n2": {"h": h, "b": {"g": g}},
    }


@pytest.mark.parametrize(
    ("targets", "input_data", "expected"),
    [
        (
            None,
            {
                "n1": {
                    "a": 1,
                    "b": 2,
                },
                "n2": {
                    "a": 3,
                    "b": {"g": 4},
                },
            },
            {"n1": {"f": 4, "g": 1}, "n2": {"h": 7}},
        ),
        (
            None,
            {
                "a": 10,
                "n1": {
                    "b": 20,
                },
                "n2": {
                    "b": {"g": 30},
                },
            },
            {"n1": {"f": 130, "g": 100}, "n2": {"h": 40}},
        ),
        (
            {"n1": {"f": None}},
            {
                "n1": {
                    "a": 100,
                    "b": 500,
                },
            },
            {"n1": {"f": 10600}},
        ),
        (
            {"n1": {"f": None}},
            {
                "a": 100,
                "n1": {
                    "b": 500,
                },
            },
            {"n1": {"f": 10600}},
        ),
    ],
)
def test_concatenate_functions_tree_simple(
    functions_simple: NestedFunctionDict,
    targets: NestedTargetDict,
    input_data: NestedInputDict,
    expected: NestedOutputDict,
) -> None:
    f = concatenate_functions_tree(
        functions=functions_simple,
        inputs=input_data,
        targets=targets,
        enforce_signature=True,
    )
    assert f(input_data) == expected


@pytest.mark.parametrize(
    ("targets", "input_data", "expected"),
    [
        (
            None,
            {
                "n1": {
                    "a": 1,
                    "b": 2,
                },
                "n2": {
                    "a": 2,
                    "b": {"a": 3},
                },
            },
            {"n1": {"f": 4, "g": 1}, "n2": {"h": 11, "b": {"g": 9}}},
        ),
        (
            None,
            {
                "a": 5,
                "n1": {
                    "b": 3,
                },
            },
            {"n1": {"f": 33, "g": 25}, "n2": {"h": 30, "b": {"g": 25}}},
        ),
    ],
)
def test_concatenate_functions_tree_nested_and_duplicate_g(
    functions_nested_and_duplicate_g: NestedFunctionDict,
    targets: NestedTargetDict,
    input_data: NestedInputDict,
    expected: NestedOutputDict,
) -> None:
    f = concatenate_functions_tree(
        functions=functions_nested_and_duplicate_g,
        targets=targets,
        inputs=input_data,
        enforce_signature=True,
    )
    assert f(input_data) == expected


def test_partialled_function_argument() -> None:
    def f(a, b):
        return a + b

    f_partial = functools.partial(f, b=1)
    tree = {"f": f_partial}
    input_structure = {"a": None}
    targets = {"f": None}

    concatenated_func = concatenate_functions_tree(
        functions=tree,
        targets=targets,
        inputs=input_structure,
        enforce_signature=True,
    )
    assert concatenated_func({"a": 1}) == {"f": 2}
