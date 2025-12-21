from __future__ import annotations

import functools
import inspect
from typing import Literal

import numpy as np
import pytest
from numpy.typing import NDArray

from dags.annotations import get_annotations, verify_annotations_are_strings
from dags.dag import concatenate_functions
from dags.exceptions import AnnotationMismatchError, NonStringAnnotationError


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


def test_verify_annotations_are_strings() -> None:
    verify_annotations_are_strings({"a": "int", "return": "float"}, "f")


def test_verify_annotations_are_strings_non_string_argument_full_message() -> None:
    with pytest.raises(
        NonStringAnnotationError,
        match=(
            r"All function annotations must be strings. The annotations for the "
            r"argument \(a\) are not strings.\n"
            r"A simple way for Python to treat type annotations as strings is to add\n"
            r"\n\tfrom __future__ import annotations\n\n"
            r"at the top of your file. Alternatively, you can do it manually by "
            r"enclosing the annotations in quotes:\n\n"
            r"\tf\(a: 'int'\) -> 'float'\."
        ),
    ):
        verify_annotations_are_strings({"a": int, "return": "float"}, "f")  # ty: ignore[invalid-argument-type]


def test_verify_annotations_are_strings_multiple_non_string_argument() -> None:
    with pytest.raises(
        NonStringAnnotationError,
        match=(
            "All function annotations must be strings. The annotations for the "
            r"arguments \(a, b\) are not strings."
        ),
    ):
        verify_annotations_are_strings({"a": int, "b": bool, "return": "float"}, "f")  # ty: ignore[invalid-argument-type]


def test_verify_annotations_are_strings_non_string_return() -> None:
    with pytest.raises(
        NonStringAnnotationError,
        match=(
            r"All function annotations must be strings. The annotations for the return"
            r" value are not strings."
        ),
    ):
        verify_annotations_are_strings({"a": "int", "return": float}, "f")  # ty: ignore[invalid-argument-type]


def test_verify_annotations_are_strings_non_string_argument_and_return() -> None:
    with pytest.raises(
        NonStringAnnotationError,
        match=(
            "All function annotations must be strings. The annotations for the argument"
            r" \(a\) and the return value are not strings."
        ),
    ):
        verify_annotations_are_strings({"a": int, "return": float}, "f")  # ty: ignore[invalid-argument-type]
