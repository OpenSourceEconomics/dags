from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

import pytest

from dags.output import (
    aggregated_output,
    dict_output,
    list_output,
    single_output,
)

if TYPE_CHECKING:
    from dags.typing import GenericCallable


@pytest.fixture
def f():
    def _f(foo: bool):  # type: ignore[no-untyped-def]
        return (int(foo), 2.0)

    # We need to set the annotations via __annotations__, because if we set it directly
    # during the function definition, the return annotation will be converted to a
    # single string, because of the from __future__ import annotations statement.
    _f.__annotations__ = {"foo": "bool", "return": ("int", "float")}
    return _f


def test_single_output(f: GenericCallable) -> None:
    g = single_output(f)  # type: ignore[var-annotated]
    assert g(foo=True) == 1


def test_single_output_annotations(f: GenericCallable) -> None:
    g = single_output(f, set_annotations=True)  # type: ignore[var-annotated]
    assert inspect.get_annotations(g) == {"foo": "bool", "return": "int"}


def test_dict_output(f: GenericCallable) -> None:
    g = dict_output(f, keys=["a", "b"])
    assert g(foo=True) == {"a": 1, "b": 2.0}


def test_dict_output_annotations(f: GenericCallable) -> None:
    g = dict_output(f, keys=["a", "b"], set_annotations=True)
    assert inspect.get_annotations(g) == {
        "foo": "bool",
        "return": {"a": "int", "b": "float"},
    }


def test_list_output(f: GenericCallable) -> None:
    g = list_output(f)
    assert g(foo=True) == [1, 2.0]


def test_list_output_annotations(f: GenericCallable) -> None:
    g = list_output(f, set_annotations=True)
    assert inspect.get_annotations(g) == {"foo": "bool", "return": ["int", "float"]}


def test_aggregated_output_decorator(f: GenericCallable) -> None:
    g = aggregated_output(f, aggregator=lambda x, y: x + y)
    assert g(foo=True) == 3


def test_single_output_direct_call(f: GenericCallable) -> None:
    g = single_output(f)  # type: ignore[var-annotated]
    assert g(foo=True) == 1


def test_single_output_decorator() -> None:
    @single_output  # type: ignore[arg-type]
    def f():
        return (1, 2)

    assert f() == 1


def test_dict_output_decorator() -> None:
    @dict_output(keys=["a", "b"])
    def f():
        return (1, 2)

    assert f() == {"a": 1, "b": 2}


def test_list_output_decorator() -> None:
    @list_output
    def f():
        return (1, 2)

    assert f() == [1, 2]
