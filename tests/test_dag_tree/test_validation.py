"""Tests for the validation module in dag_tree."""

from __future__ import annotations

from typing import Literal

import pytest

from dags.exceptions import (
    RepeatedTopLevelElementError,
    TrailingUnderscoreError,
)
from dags.tree.validation import (
    fail_if_path_elements_have_trailing_undersores,
    fail_if_top_level_elements_repeated_in_paths,
    fail_if_top_level_elements_repeated_in_single_path,
)


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
        with pytest.raises(
            TrailingUnderscoreError,
            match=(
                "Except for the leaf name, elements of the paths in the functions tree "
                "must not end with an underscore."
            ),
        ):
            fail_if_path_elements_have_trailing_undersores(tree_paths)
    else:
        fail_if_path_elements_have_trailing_undersores(tree_paths)


@pytest.mark.parametrize(
    ("top_level_namespace", "all_tree_paths", "expected"),
    [
        ({"a"}, {("a",), ("a", "b")}, "pass"),
        ({"a", "b"}, {("a",), ("b", "a")}, "raise"),
        ({"a"}, {("a",), ("b", "a", "c")}, "raise"),
        ({"a"}, {("a",), ("b", "c", "a")}, "raise"),
    ],
)
def test_fail_if_top_level_elements_repeated_in_paths(
    top_level_namespace: set[str],
    all_tree_paths: set[tuple[str, ...]],
    expected: Literal["raise", "pass"],
) -> None:
    if expected == "raise":
        with pytest.raises(
            RepeatedTopLevelElementError,
            match="Elements of the top-level namespace must not be repeated",
        ):
            fail_if_top_level_elements_repeated_in_paths(
                all_tree_paths=all_tree_paths,
                top_level_namespace=top_level_namespace,
            )
    else:
        fail_if_top_level_elements_repeated_in_paths(
            all_tree_paths=all_tree_paths,
            top_level_namespace=top_level_namespace,
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
            RepeatedTopLevelElementError,
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
