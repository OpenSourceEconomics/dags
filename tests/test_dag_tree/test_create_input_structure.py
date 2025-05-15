"""Tests for the dag_tree module."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from dags.tree import RepeatedTopLevelElementError, create_input_structure_tree

if TYPE_CHECKING:
    from dags.tree.typing import (
        NestedFunctionDict,
        NestedInputStructureDict,
        NestedTargetDict,
    )


def f(g: int, a: int, b: float, c) -> float:
    # We expect to see "unknown_type" for c
    return g + a + b + c


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
    ("targets", "top_level_inputs", "expected"),
    [
        (
            None,
            set(),
            {
                "n1": {
                    "a": "int",
                    "b": "float",
                    "c": "unknown_type",
                },
                "n2": {
                    "a": "int",
                    "b": {"g": "int"},
                },
            },
        ),
        (
            None,
            {"a"},
            {
                "a": "int",
                "n1": {
                    "b": "float",
                    "c": "unknown_type",
                },
                "n2": {
                    "b": {"g": "int"},
                },
            },
        ),
        (
            {"n1": {"f": "float"}},
            set(),
            {
                "n1": {
                    "a": "int",
                    "b": "float",
                    "c": "unknown_type",
                },
            },
        ),
        (
            {"n1": {"f": "float"}},
            {"a"},
            {
                "a": "int",
                "n1": {
                    "b": "float",
                    "c": "unknown_type",
                },
            },
        ),
        (
            None,
            {"a", "g"},
            {"raises_error": None},
        ),
    ],
)
def test_create_input_structure_tree_simple(
    functions_simple: NestedFunctionDict,
    targets: NestedTargetDict | None,
    top_level_inputs: set[str],
    expected: NestedInputStructureDict,
) -> None:
    if "raises_error" in expected:
        with pytest.raises(RepeatedTopLevelElementError):
            create_input_structure_tree(functions_simple, targets, top_level_inputs)
    else:
        assert (
            create_input_structure_tree(functions_simple, targets, top_level_inputs)
            == expected
        )


@pytest.mark.parametrize(
    ("targets", "top_level_inputs", "expected"),
    [
        (
            None,
            set(),
            {
                "n1": {
                    "a": "int",
                    "b": "float",
                    "c": "unknown_type",
                },
                "n2": {
                    "a": "int",
                    "b": {"a": "int"},
                },
            },
        ),
        (
            None,
            {"a"},
            {
                "a": "int",
                "n1": {
                    "b": "float",
                    "c": "unknown_type",
                },
            },
        ),
        (
            None,
            {"a", "g"},
            {"raises_error": None},
        ),
    ],
)
def test_create_input_structure_tree_nested_and_duplicate_g(
    functions_nested_and_duplicate_g: NestedFunctionDict,
    targets: NestedTargetDict | None,
    top_level_inputs: set[str],
    expected: NestedInputStructureDict,
) -> None:
    if "raises_error" in expected:
        with pytest.raises(RepeatedTopLevelElementError):
            create_input_structure_tree(
                functions_nested_and_duplicate_g, targets, top_level_inputs
            )
    else:
        assert (
            create_input_structure_tree(
                functions_nested_and_duplicate_g, targets, top_level_inputs
            )
            == expected
        )


def test_create_input_structure_tree_duplicates_lower_in_hierarchy() -> None:
    assert create_input_structure_tree(
        functions={
            "n1": {"a": {"f": f}},
        },
        targets=None,
        top_level_inputs=set(),
    ) == {"n1": {"a": {"a": "int", "b": "float", "g": "int", "c": "unknown_type"}}}
