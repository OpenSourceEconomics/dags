from __future__ import annotations

import functools

import pytest

from dags.annotations import get_annotations
from dags.dag import _verify_annotations_are_strings, concatenate_functions
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
            "function g has the argument type annotation 'a: float', but "
            "type annotation 'a: int' is used elsewhere."
        ),
    ):
        concatenate_functions([f, g], set_annotations=True)


def test_argument_annoations_mismatch_with_return_annotation() -> None:
    """Problem: f expects g: int, but g() returns float."""

    def f(g: int) -> int:
        return g

    def g() -> float:
        return 1.0

    with pytest.raises(
        AnnotationMismatchError,
        match=(
            "function f has the argument type annotation 'g: int', but function g has "
            "return type: float."
        ),
    ):
        concatenate_functions([f, g], set_annotations=True)


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
    def f(a) -> float:  # type: ignore[no-untyped-def]
        return float(a)

    assert get_annotations(f, default="default") == {"a": "default", "return": "float"}
    assert get_annotations(f, default=bool, eval_str=True) == {
        "a": bool,
        "return": float,
    }


def test_verify_annotations_are_strings() -> None:
    _verify_annotations_are_strings({"a": "int", "return": "float"}, "f")


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
        _verify_annotations_are_strings({"a": int, "return": "float"}, "f")  # type: ignore[dict-item]


def test_verify_annotations_are_strings_multiple_non_string_argument() -> None:
    with pytest.raises(
        NonStringAnnotationError,
        match=(
            "All function annotations must be strings. The annotations for the "
            r"arguments \(a, b\) are not strings."
        ),
    ):
        _verify_annotations_are_strings({"a": int, "b": bool, "return": "float"}, "f")  # type: ignore[dict-item]


def test_verify_annotations_are_strings_non_string_return() -> None:
    with pytest.raises(
        NonStringAnnotationError,
        match=(
            "All function annotations must be strings. The annotations for the return"
            " value are not strings."
        ),
    ):
        _verify_annotations_are_strings({"a": "int", "return": float}, "f")  # type: ignore[dict-item]


def test_verify_annotations_are_strings_non_string_argument_and_return() -> None:
    with pytest.raises(
        NonStringAnnotationError,
        match=(
            "All function annotations must be strings. The annotations for the argument"
            r" \(a\) and the return value are not strings."
        ),
    ):
        _verify_annotations_are_strings({"a": int, "return": float}, "f")  # type: ignore[dict-item]
