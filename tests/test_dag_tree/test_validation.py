"""Tests for the validation module in dag_tree."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import pytest

from dags.dag_tree.validation import (
    _check_for_parent_child_name_clashes,
    _find_parent_child_name_clashes,
)

if TYPE_CHECKING:
    from dags.dag_tree import (
        FlatFunctionDict,
        FlatInputStructureDict,
    )


@pytest.mark.parametrize(
    ("functions", "input_structure", "name_clashes"),
    [
        ({"x": lambda x: x, "nested__x": lambda x: x}, {}, "raise"),
        ({"nested__x": lambda x: x, "nested__deep__x": lambda x: x}, {}, "raise"),
        ({"x": lambda x: x}, {"nested__x": None}, "raise"),
        ({}, {"x": None, "nested__x": None}, "raise"),
    ],
)
def test_check_for_parent_child_name_clashes_error(
    functions: FlatFunctionDict,
    input_structure: FlatInputStructureDict,
    name_clashes: Literal["raise"],
) -> None:
    with pytest.raises(ValueError, match="There are name clashes:"):
        _check_for_parent_child_name_clashes(functions, input_structure, name_clashes)


@pytest.mark.parametrize(
    ("functions", "input_structure", "name_clashes"),
    [
        ({"x": lambda x: x, "nested__x": lambda x: x}, {}, "warn"),
        ({"nested__x": lambda x: x, "nested__deep__x": lambda x: x}, {}, "warn"),
        ({"x": lambda x: x}, {"nested__x": None}, "warn"),
        ({}, {"x": None, "nested__x": None}, "warn"),
    ],
)
def test_check_for_parent_child_name_clashes_warn(
    functions: FlatFunctionDict,
    input_structure: FlatInputStructureDict,
    name_clashes: Literal["warn"],
) -> None:
    with pytest.warns(UserWarning, match="There are name clashes:"):
        _check_for_parent_child_name_clashes(functions, input_structure, name_clashes)


@pytest.mark.parametrize(
    ("functions", "input_structure", "name_clashes"),
    [
        ({"x": lambda x: x, "nested__x": lambda x: x}, {}, "ignore"),
        ({"nested__x": lambda x: x, "nested__deep__x": lambda x: x}, {}, "ignore"),
        ({"x": lambda x: x}, {"nested__x": None}, "ignore"),
        ({}, {"x": None, "nested__x": None}, "ignore"),
        ({"x": lambda x: x}, {"y": None}, "raise"),
    ],
)
def test_check_for_parent_child_name_clashes_no_error(
    functions: FlatFunctionDict,
    input_structure: FlatInputStructureDict,
    name_clashes: Literal["raise", "ignore"],
) -> None:
    _check_for_parent_child_name_clashes(functions, input_structure, name_clashes)


@pytest.mark.parametrize(
    ("functions", "input_structure", "expected"),
    [
        ({"x": lambda x: x, "nested__x": lambda x: x}, {}, [("nested__x", "x")]),
        (
            {"nested__x": lambda x: x, "nested__deep__x": lambda x: x},
            {},
            [("nested__x", "nested__deep__x")],
        ),
        ({"x": lambda x: x}, {"nested__x": None}, [("nested__x", "x")]),
        ({}, {"x": None, "nested__x": None}, [("nested__x", "x")]),
        ({"x": lambda x: x}, {"y": None}, []),
    ],
)
def test_find_parent_child_name_clashes(
    functions: FlatFunctionDict,
    input_structure: FlatInputStructureDict,
    expected: list[tuple[str, str]],
) -> None:
    actual = _find_parent_child_name_clashes(functions, input_structure)

    unordered_expected = [set(pair) for pair in expected]
    unordered_actual = [set(pair) for pair in actual]

    assert unordered_actual == unordered_expected
