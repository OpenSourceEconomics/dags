"""Tests for deprecated shims in dags.tree."""

import pytest

from dags.tree import (
    fail_if_paths_are_invalid,
    functions_without_tree_logic,
    one_function_without_tree_logic,
)
from dags.tree.typing import NestedFunctionDict


def f(a: int) -> int:
    return a**2


def g(f: int) -> int:
    return f + 1


FUNCTIONS: NestedFunctionDict = {"ns": {"f": f, "g": g}}
TOP_LEVEL_NAMESPACE: set[str] = {"ns"}


def test_one_function_without_tree_logic_emits_warning() -> None:
    with pytest.warns(FutureWarning, match="one_function_without_tree_logic"):
        one_function_without_tree_logic(
            function=f,
            tree_path=("ns", "f"),
            top_level_namespace=TOP_LEVEL_NAMESPACE,
        )


def test_functions_without_tree_logic_emits_warning() -> None:
    with pytest.warns(FutureWarning, match="functions_without_tree_logic"):
        functions_without_tree_logic(
            functions=FUNCTIONS,
            top_level_namespace=TOP_LEVEL_NAMESPACE,
        )


def test_fail_if_paths_are_invalid_emits_warning() -> None:
    with pytest.warns(FutureWarning, match="fail_if_paths_are_invalid"):
        fail_if_paths_are_invalid(functions=FUNCTIONS)
