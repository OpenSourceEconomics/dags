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


def f(g, a, b):
    return g + a + b


def g(a):
    return a**2


def h(a, b__g):
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
                    "a": None,
                    "b": None,
                },
                "n2": {
                    "a": None,
                    "b": {"g": None},
                },
            },
        ),
        (
            None,
            {"a"},
            {
                "a": None,
                "n1": {
                    "b": None,
                },
                "n2": {
                    "b": {"g": None},
                },
            },
        ),
        (
            {"n1": {"f": None}},
            set(),
            {
                "n1": {
                    "a": None,
                    "b": None,
                },
            },
        ),
        (
            {"n1": {"f": None}},
            {"a"},
            {
                "a": None,
                "n1": {
                    "b": None,
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
                    "a": None,
                    "b": None,
                },
                "n2": {
                    "a": None,
                    "b": {"a": None},
                },
            },
        ),
        (
            None,
            {"a"},
            {
                "a": None,
                "n1": {
                    "b": None,
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
    ) == {"n1": {"a": {"a": None, "b": None, "g": None}}}
