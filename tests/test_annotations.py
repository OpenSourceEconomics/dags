from __future__ import annotations

import functools

import pytest

from dags.annotations import get_annotations
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
