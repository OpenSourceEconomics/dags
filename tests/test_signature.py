from __future__ import annotations

import inspect

import pytest

from dags.exceptions import InvalidFunctionArgumentsError
from dags.signature import _create_signature, rename_arguments, with_signature


@pytest.fixture
def example_signature() -> inspect.Signature:
    parameters = [
        inspect.Parameter(name="a", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD),
        inspect.Parameter(name="b", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD),
        inspect.Parameter(name="c", kind=inspect.Parameter.KEYWORD_ONLY),
    ]
    return inspect.Signature(parameters=parameters)


@pytest.fixture
def example_signature_annotated() -> inspect.Signature:
    parameters = [
        inspect.Parameter(
            name="a", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=int
        ),
        inspect.Parameter(
            name="b", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=float
        ),
        inspect.Parameter(
            name="c", kind=inspect.Parameter.KEYWORD_ONLY, annotation=bool
        ),
    ]
    return inspect.Signature(parameters=parameters, return_annotation=float)


def test_create_signature(example_signature: inspect.Signature) -> None:
    created = _create_signature(
        args_types={"a": inspect.Parameter.empty, "b": inspect.Parameter.empty},
        kwargs_types={"c": inspect.Parameter.empty},
    )
    assert created == example_signature


def test_create_signature_annotated(
    example_signature_annotated: inspect.Signature,
) -> None:
    created = _create_signature(
        args_types={"a": "int", "b": "float"},
        kwargs_types={"c": "bool"},
        return_annotation="float",
    )
    assert created == example_signature_annotated


def test_with_signature_decorator_valid(example_signature: inspect.Signature) -> None:
    @with_signature(args=["a", "b"], kwargs=["c"])
    def f(*args, **kwargs):
        return sum(args) + sum(kwargs.values())

    assert inspect.signature(f) == example_signature

    assert f(1, 2, c=3) == 6


def test_with_signature_decorator_valid_annotated(
    example_signature_annotated: inspect.Signature,
) -> None:
    @with_signature(
        args={"a": "int", "b": "float"}, kwargs={"c": "bool"}, return_annotation="float"
    )
    def f(*args, **kwargs):
        return sum(args) + sum(kwargs.values())

    assert inspect.signature(f) == example_signature_annotated


def test_with_signature_direct_call_valid(example_signature: inspect.Signature) -> None:
    def f(*args, **kwargs):
        return sum(args) + sum(kwargs.values())

    g = with_signature(f, args=["a", "b"], kwargs=["c"])

    assert inspect.signature(g) == example_signature

    assert g(1, 2, c=3) == 6


def test_with_signature_direct_call_valid_annotated(
    example_signature_annotated: inspect.Signature,
) -> None:
    def f(*args, **kwargs):
        return sum(args) + sum(kwargs.values())

    g = with_signature(
        f,
        args={"a": "int", "b": "float"},
        kwargs={"c": "bool"},
        return_annotation="float",
    )

    assert inspect.signature(g) == example_signature_annotated


def test_with_signature_args_used_as_kwargs() -> None:
    @with_signature(args=["a", "b"], kwargs=["c"])
    def f(*args, **kwargs):
        return sum(args) + sum(kwargs.values())

    assert f(a=1, b=2, c=3) == 6


def test_with_signature_decorator_no_enforcing(
    example_signature: inspect.Signature,
) -> None:
    @with_signature(args=["a", "b"], kwargs=["c"], enforce=False)
    def f(*args, **kwargs):
        return sum(args) + sum(kwargs.values())

    assert inspect.signature(f) == example_signature

    assert f(x=3) == 3


def test_with_signature_decorator_too_many_positional_arguments() -> None:
    @with_signature(args=["a", "b"], kwargs=["c"])
    def f(*args, **kwargs):
        return sum(args) + sum(kwargs.values())

    with pytest.raises(
        InvalidFunctionArgumentsError,
        match="takes 2 positional arguments but 3 were given",
    ):
        f(1, 2, 3)


def test_with_signature_decorator_duplicated_arguments() -> None:
    @with_signature(args=["a", "b"], kwargs=["c"])
    def f(*args, **kwargs):
        return sum(args) + sum(kwargs.values())

    with pytest.raises(
        InvalidFunctionArgumentsError,
        match=r"f\(\) got multiple values for argument b",
    ):
        f(1, 2, b=3)


def test_with_signature_decorator_invalid_keyword_arguments() -> None:
    @with_signature(args=["a", "b"], kwargs=["c"])
    def f(*args, **kwargs):
        return sum(args) + sum(kwargs.values())

    with pytest.raises(
        InvalidFunctionArgumentsError,
        match=r"f\(\) got unexpected keyword argument d",
    ):
        f(1, 2, d=4)


def test_rename_arguments_decorator(example_signature: inspect.Signature) -> None:
    @rename_arguments(mapper={"e": "b", "d": "a", "f": "c"})
    def f(d, e, *, f):
        return (d, e, f)

    assert inspect.signature(f) == example_signature

    # Note: mypy can't handle the rename arguments here.
    assert f(b=2, c=3, a=1) == (1, 2, 3)  # type: ignore[call-arg]


def test_rename_arguments_decorator_annotated() -> None:
    @rename_arguments(mapper={"e": "b", "d": "a", "f": "c"})
    def f(d: int, e: float, *, f: bool) -> float:
        return d + e + f

    assert inspect.get_annotations(f, eval_str=True) == {
        "a": int,
        "b": float,
        "c": bool,
        "return": float,
    }


def test_rename_arguments_direct_call(example_signature: inspect.Signature) -> None:
    def f(d, e, *, f):
        return (d, e, f)

    g = rename_arguments(f, mapper={"e": "b", "d": "a", "f": "c"})

    assert inspect.signature(g) == example_signature

    # Note: mypy can't handle the rename arguments here.
    assert g(b=2, c=3, a=1) == (1, 2, 3)  # type: ignore[call-arg]


def test_rename_arguments_direct_call_annotated() -> None:
    def f(d: int, e: float, *, f: bool) -> float:
        return d + e + f

    g = rename_arguments(f, mapper={"e": "b", "d": "a", "f": "c"})

    assert inspect.get_annotations(g, eval_str=True) == {
        "a": int,
        "b": float,
        "c": bool,
        "return": float,
    }
