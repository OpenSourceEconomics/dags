# Required because tests assert that annotations are strings.
from __future__ import annotations

import functools
import inspect

import pytest

from dags.annotations import get_annotations
from dags.exceptions import DagsError, InvalidFunctionArgumentsError
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
            name="a", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation="int"
        ),
        inspect.Parameter(
            name="b", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation="float"
        ),
        inspect.Parameter(
            name="c", kind=inspect.Parameter.KEYWORD_ONLY, annotation="bool"
        ),
    ]
    return inspect.Signature(parameters=parameters, return_annotation="float")


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


def test_with_signature_advertises_forwarder_shape_on_annotations() -> None:
    """`with_signature` advertises the `*args, **kwargs` forwarder on `__annotations__`.

    The wrapper is a generic `*args, **kwargs` forwarder; its
    `__annotations__` says so (`{"args": object, "kwargs": object}`).
    Runtime type checkers reading `__annotations__` (beartype, typeguard,
    `typing.get_type_hints`) therefore treat the wrapper as permissive and
    do not enforce the wrapped function's per-parameter annotations.
    """

    @with_signature(args={"a": "int"}, kwargs={"b": "float"}, return_annotation="float")
    def f(*args, **kwargs):
        return sum(args) + sum(kwargs.values())

    assert f.__annotations__ == {"args": object, "kwargs": object}
    assert "a" not in f.__annotations__
    assert "b" not in f.__annotations__
    assert "return" not in f.__annotations__


def test_with_signature_keeps_user_view_on_signature() -> None:
    """`with_signature` keeps the user-described view on `__signature__`.

    Introspection tools using `inspect.signature` see parameter names,
    kinds, and the type strings passed via `args` / `kwargs`, even though
    `__annotations__` only carries the forwarder shape.
    """

    @with_signature(args={"a": "int"}, kwargs={"b": "float"}, return_annotation="float")
    def f(*args, **kwargs):
        return sum(args) + sum(kwargs.values())

    sig = inspect.signature(f)
    assert sig.parameters["a"].annotation == "int"
    assert sig.parameters["b"].annotation == "float"
    assert sig.return_annotation == "float"


def test_with_signature_get_annotations_recovers_user_view() -> None:
    """`dags.get_annotations` recovers the user view from a `with_signature` wrapper.

    The args/kwargs-mismatch fallback recognises the forwarder shape on
    `__annotations__` and reads the user-described view off `__signature__`
    instead, so dags' own machinery (DAG resolution, signature tooling)
    keeps working on wrappers that advertise the forwarder shape.
    """

    @with_signature(args={"a": "int"}, kwargs={"b": "float"}, return_annotation="float")
    def f(*args, **kwargs):
        return sum(args) + sum(kwargs.values())

    recovered = get_annotations(f)
    assert recovered["a"] == "int"
    assert recovered["b"] == "float"
    assert recovered["return"] == "float"


def test_rename_arguments_advertises_forwarder_shape_on_annotations() -> None:
    """`rename_arguments` advertises the forwarder shape on `__annotations__`.

    Same semantics as `with_signature`: the wrapper is a forwarder, so its
    `__annotations__` is the forwarder shape and the renamed user view
    lives on `__signature__`.
    """

    def f(a: int, b: float) -> float:
        return a + b

    renamed = rename_arguments(f, mapper={"a": "x"})

    assert renamed.__annotations__ == {"args": object, "kwargs": object}
    assert "x" not in renamed.__annotations__
    sig = inspect.signature(renamed)
    assert "x" in sig.parameters
    assert "b" in sig.parameters
    assert sig.return_annotation == "float"


def test_rename_arguments_get_annotations_recovers_user_view() -> None:
    """`dags.get_annotations` recovers the renamed view from the wrapper."""

    def f(a: int, b: float) -> float:
        return a + b

    renamed = rename_arguments(f, mapper={"a": "x"})

    recovered = get_annotations(renamed)
    assert recovered["x"] == "int"
    assert recovered["b"] == "float"
    assert recovered["return"] == "float"
    sig = inspect.signature(renamed)
    assert "x" in sig.parameters
    assert "b" in sig.parameters


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
        match=r"f got multiple values for argument b",
    ):
        f(1, 2, b=3)


def test_with_signature_decorator_invalid_keyword_arguments() -> None:
    @with_signature(args=["a", "b"], kwargs=["c"])
    def f(*args, **kwargs):
        return sum(args) + sum(kwargs.values())

    with pytest.raises(
        InvalidFunctionArgumentsError,
        match=r"f got unexpected keyword argument d",
    ):
        f(1, 2, d=4)


def test_rename_arguments_decorator(example_signature: inspect.Signature) -> None:
    @rename_arguments(mapper={"e": "b", "d": "a", "f": "c"})
    def f(d, e, *, f):
        return (d, e, f)

    assert inspect.signature(f) == example_signature

    assert f(b=2, c=3, a=1) == (1, 2, 3)


def test_rename_arguments_decorator_annotated() -> None:
    @rename_arguments(mapper={"e": "b", "d": "a", "f": "c"})
    def f(d: int, e: float, *, f: bool) -> float:
        return d + e + f

    # `__annotations__` carries the forwarder shape; the renamed user view
    # is recovered via `dags.get_annotations` (which falls back to
    # `__signature__`).
    assert f.__annotations__ == {"args": object, "kwargs": object}
    assert get_annotations(f) == {
        "a": "int",
        "b": "float",
        "c": "bool",
        "return": "float",
    }


def test_rename_arguments_direct_call(example_signature: inspect.Signature) -> None:
    def f(d, e, *, f):
        return (d, e, f)

    g = rename_arguments(f, mapper={"e": "b", "d": "a", "f": "c"})

    assert inspect.signature(g) == example_signature

    assert g(b=2, c=3, a=1) == (1, 2, 3)


def test_rename_arguments_direct_call_annotated() -> None:
    def f(d: int, e: float, *, f: bool) -> float:
        return d + e + f

    g = rename_arguments(f, mapper={"e": "b", "d": "a", "f": "c"})

    # `__annotations__` carries the forwarder shape; the renamed user view
    # is recovered via `dags.get_annotations`.
    assert g.__annotations__ == {"args": object, "kwargs": object}
    assert get_annotations(g) == {
        "a": "int",
        "b": "float",
        "c": "bool",
        "return": "float",
    }


def test_with_signature_decorator_missing_arguments() -> None:
    @with_signature(args=["a", "b"], kwargs=["c"])
    def f(*args, **kwargs):
        return sum(args) + sum(kwargs.values())

    with pytest.raises(
        InvalidFunctionArgumentsError,
        match="missing required argument",
    ):
        f(1)


def test_with_signature_decorator_missing_all_arguments() -> None:
    @with_signature(args=["a", "b"], kwargs=["c"])
    def f(*args, **kwargs):
        return sum(args) + sum(kwargs.values())

    with pytest.raises(
        InvalidFunctionArgumentsError,
        match=r"missing required arguments.*a.*b",
    ):
        f()


def test_with_signature_invalid_args_type() -> None:
    with pytest.raises(DagsError, match="Invalid type for arg"):

        @with_signature(args="invalid")
        def f(*args, **kwargs):
            pass


def test_with_signature_invalid_args_type_int() -> None:
    with pytest.raises(DagsError, match="Invalid type for arg"):

        @with_signature(args=42)  # ty: ignore[invalid-argument-type]
        def f(*args, **kwargs):
            pass


def test_rename_arguments_partial_with_positional_bound_arg() -> None:
    def f(a, b):
        return a + b

    p = functools.partial(f, 1)
    renamed = rename_arguments(p, mapper={"b": "x"})
    assert renamed(x=2) == 3


def test_rename_arguments_partial_with_keyword_bound_arg() -> None:
    def f(a, b):
        return a * b

    p = functools.partial(f, b=10)
    renamed = rename_arguments(p, mapper={"a": "x"})
    assert renamed(x=5) == 50


def test_rename_arguments_partial_positional_call() -> None:
    def f(a, b):
        return f"{a}-{b}"

    p = functools.partial(f, "hello")
    renamed = rename_arguments(p, mapper={"b": "x"})
    assert renamed("world") == "hello-world"


def test_rename_arguments_partial_bound_arg_not_in_mapper() -> None:
    def f(a, b, c):
        return a + b + c

    p = functools.partial(f, 100)
    renamed = rename_arguments(p, mapper={"b": "x"})
    assert renamed(x=20, c=3) == 123


def test_rename_arguments_partial_multiple_bound_args() -> None:
    def f(a, b, c):
        return a + b + c

    p = functools.partial(f, 10, c=30)
    renamed = rename_arguments(p, mapper={"b": "y"})
    assert renamed(y=20) == 60


def test_rename_arguments_decorator_on_partial() -> None:
    def f(a, b):
        return a + b

    p = functools.partial(f, 1)
    renamed = rename_arguments(mapper={"b": "x"})(p)
    assert renamed(x=2) == 3
