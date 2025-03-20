"""Tests for the validation module in dag_tree."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import pytest

from dags.tree.validation import (
    _check_for_parent_child_name_clashes,
    _find_parent_child_name_clashes,
    fail_if_path_elements_have_trailing_undersores,
    fail_if_top_level_elements_repeated_in_paths,
    fail_if_top_level_elements_repeated_in_single_path,
)

if TYPE_CHECKING:
    from dags.tree.typing import (
        QualNameFunctionDict,
        QualNameInputStructureDict,
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
    functions: QualNameFunctionDict,
    input_structure: QualNameInputStructureDict,
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
    functions: QualNameFunctionDict,
    input_structure: QualNameInputStructureDict,
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
    functions: QualNameFunctionDict,
    input_structure: QualNameInputStructureDict,
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
    functions: QualNameFunctionDict,
    input_structure: QualNameInputStructureDict,
    expected: list[tuple[str, str]],
) -> None:
    actual = _find_parent_child_name_clashes(functions, input_structure)

    unordered_expected = [set(pair) for pair in expected]
    unordered_actual = [set(pair) for pair in actual]

    assert unordered_actual == unordered_expected


@pytest.mark.parametrize(
    ("tree_paths", "expected"),
    [
        ({("a",), ("b", "a")}, "pass"),
        ({("a_",), ("b", "a_")}, "pass"),
        ({("a",), ("b_", "a")}, "raise"),
    ],
)
def test_fail_if_path_elements_have_trailing_undersores(
    tree_paths: set[tuple[str, ...]],
    expected: Literal["raise", "pass"],
) -> None:
    if expected == "raise":
        with pytest.raises(ValueError, match="Except for the leaf name, elements"):
            fail_if_path_elements_have_trailing_undersores(tree_paths)
    else:
        fail_if_path_elements_have_trailing_undersores(tree_paths)


@pytest.mark.parametrize(
    ("top_level_namespace", "tree_paths", "expected"),
    [
        ({"a"}, {("a",), ("a", "b")}, "pass"),
        ({"a", "b"}, {("a",), ("b", "a")}, "raise"),
        ({"a"}, {("a",), ("b", "a", "c")}, "raise"),
        ({"a"}, {("a",), ("b", "c", "a")}, "raise"),
    ],
)
def test_fail_if_top_level_elements_repeated_in_paths(
    top_level_namespace: set[str],
    tree_paths: set[tuple[str, ...]],
    expected: Literal["raise", "pass"],
) -> None:
    if expected == "raise":
        with pytest.raises(
            ValueError,
            match="Elements of the top-level namespace must not be repeated",
        ):
            fail_if_top_level_elements_repeated_in_paths(
                top_level_namespace=top_level_namespace,
                tree_paths=tree_paths,
            )
    else:
        fail_if_top_level_elements_repeated_in_paths(
            top_level_namespace=top_level_namespace,
            tree_paths=tree_paths,
        )


@pytest.mark.parametrize(
    ("top_level_namespace", "tree_path", "expected"),
    [
        (
            {"a"},
            ("a", "b"),
            "pass",
        ),
        ({"a", "b"}, ("a",), "pass"),
        ({"a", "b"}, ("b", "a"), "raise"),
        ({"a"}, ("b", "a", "c"), "raise"),
        ({"a"}, ("b", "c", "a"), "raise"),
    ],
)
def test_fail_if_top_level_elements_repeated_in_single_path(
    top_level_namespace: set[str],
    tree_path: tuple[str, ...],
    expected: Literal["raise", "pass"],
) -> None:
    if expected == "raise":
        with pytest.raises(
            ValueError,
            match="Elements of the top-level namespace must not be repeated",
        ):
            fail_if_top_level_elements_repeated_in_single_path(
                top_level_namespace=top_level_namespace,
                tree_path=tree_path,
            )
    else:
        fail_if_top_level_elements_repeated_in_single_path(
            top_level_namespace=top_level_namespace,
            tree_path=tree_path,
        )
