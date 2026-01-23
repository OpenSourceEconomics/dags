from __future__ import annotations

import functools
import inspect
from typing import Literal

import numpy as np
import pytest
from numpy.typing import NDArray

from dags.annotations import ensure_annotations_are_strings, get_annotations
from dags.dag import concatenate_functions
from dags.exceptions import AnnotationMismatchError


def test_argument_annotations_mismatch() -> None:
    """Problem: f expects a: int, but g expects a: float."""

    def f(a: int) -> int:
        return a

    def g(a: float) -> int:
        return int(a)

    with pytest.raises(
        AnnotationMismatchError,
        match=(
            r"function g has the argument type annotation 'a: float', but "
            r"type annotation 'a: int' is used elsewhere."
        ),
    ):
        concatenate_functions(functions=[f, g], set_annotations=True)


def test_argument_annotations_mismatch_with_return_annotation() -> None:
    """Problem: f expects g: int, but g() returns float."""

    def f(g: int) -> int:
        return g

    def g() -> float:
        return 1.0

    with pytest.raises(
        AnnotationMismatchError,
        match=(
            r"function f has the argument type annotation 'g: int', but function g has "
            r"return type: float."
        ),
    ):
        concatenate_functions(functions=[f, g], set_annotations=True)


@pytest.mark.parametrize("return_type", ["tuple", "list", "dict"])
def test_concatenate_functions_without_input(
    return_type: Literal["tuple", "list", "dict"],
) -> None:
    concatenated = concatenate_functions(
        functions={},
        targets=None,
        return_type=return_type,
        set_annotations=True,
    )
    expected_type = {
        "tuple": (),
        "list": [],
        "dict": {},
    }
    assert inspect.get_annotations(concatenated, eval_str=True) == {
        "return": expected_type[return_type],
    }


def test_get_annotations() -> None:
    def f(a: int) -> float:
        return float(a)

    assert get_annotations(f) == {"a": "int", "return": "float"}
    assert get_annotations(f, eval_str=True) == {"a": int, "return": float}


def test_get_annotations_with_partial() -> None:
    def f(a: int, b: bool) -> float:
        return float(a) + int(b)

    g = functools.partial(f, b=True)

    assert get_annotations(f) == {"a": "int", "b": "bool", "return": "float"}
    assert get_annotations(g) == {"a": "int", "return": "float"}
    assert get_annotations(g, eval_str=True) == {"a": int, "return": float}


def test_get_annotations_with_default() -> None:
    def f(a) -> float:
        return float(a)

    assert get_annotations(f) == {"a": "no_annotation_found", "return": "float"}
    assert get_annotations(f, default="default") == {"a": "default", "return": "float"}
    assert get_annotations(f, default=bool, eval_str=True) == {
        "a": bool,
        "return": float,
    }


def test_get_annotations_with_numpy() -> None:
    def f(a: NDArray[np.float64]) -> float:
        return a.sum()

    assert get_annotations(f) == {"a": "NDArray[np.float64]", "return": "float"}
    assert get_annotations(f, eval_str=True) == {
        "a": NDArray[np.float64],
        "return": float,
    }


def test_ensure_annotations_are_strings_already_strings() -> None:
    """Test that string annotations are passed through unchanged."""
    result = ensure_annotations_are_strings({"a": "int", "return": "float"})
    assert result == {"a": "int", "return": "float"}


def test_ensure_annotations_are_strings_converts_non_string_argument() -> None:
    """Test that non-string argument annotations are converted to strings."""
    result = ensure_annotations_are_strings({"a": int, "return": "float"})
    assert result == {"a": "int", "return": "float"}


def test_ensure_annotations_are_strings_converts_multiple_non_string_arguments() -> (
    None
):
    """Test that multiple non-string argument annotations are converted."""
    result = ensure_annotations_are_strings({"a": int, "b": bool, "return": "float"})
    assert result == {"a": "int", "b": "bool", "return": "float"}


def test_ensure_annotations_are_strings_converts_non_string_return() -> None:
    """Test that non-string return annotation is converted to string."""
    result = ensure_annotations_are_strings({"a": "int", "return": float})
    assert result == {"a": "int", "return": "float"}


def test_ensure_annotations_are_strings_converts_all_non_strings() -> None:
    """Test that all non-string annotations are converted."""
    result = ensure_annotations_are_strings({"a": int, "return": float})
    assert result == {"a": "int", "return": "float"}


def test_get_annotations_fallback_for_args_kwargs_mismatch() -> None:
    """Test fallback to signature when annotations have args/kwargs mismatch."""

    def inner(wealth: float, flag: bool) -> float:  # noqa: ARG001
        return wealth

    @functools.wraps(inner)
    def wrapper(*args, **kwargs) -> float:
        return inner(*args, **kwargs)

    # Force the mismatch condition
    wrapper.__annotations__ = {
        "args": "P.args",
        "kwargs": "P.kwargs",
        "return": "float",
    }

    result = get_annotations(wrapper)
    assert result == {"wealth": "float", "flag": "bool", "return": "float"}
