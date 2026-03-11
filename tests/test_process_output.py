import inspect
from collections.abc import Callable
from typing import Any

import pytest

from dags.exceptions import DagsError
from dags.output import (
    aggregated_output,
    dict_output,
    list_output,
    single_output,
)


@pytest.fixture
def f():
    def _f(foo: bool):
        return (int(foo), 2.0)

    # We need to set the annotations via __annotations__ to get a tuple return
    # annotation, because Python doesn't support tuple syntax in annotation positions.
    _f.__annotations__ = {"foo": "bool", "return": ("int", "float")}
    return _f


def test_single_output(f: Callable[..., Any]) -> None:
    g = single_output(f)
    assert g(foo=True) == 1


def test_single_output_annotations(f: Callable[..., Any]) -> None:
    g = single_output(f, set_annotations=True)
    assert inspect.get_annotations(g) == {"foo": "bool", "return": "int"}


def test_dict_output(f: Callable[..., Any]) -> None:
    g = dict_output(f, keys=["a", "b"])
    assert g(foo=True) == {"a": 1, "b": 2.0}


def test_dict_output_annotations(f: Callable[..., Any]) -> None:
    g = dict_output(f, keys=["a", "b"], set_annotations=True)
    assert inspect.get_annotations(g) == {
        "foo": "bool",
        "return": {"a": "int", "b": "float"},
    }


def test_list_output(f: Callable[..., Any]) -> None:
    g = list_output(f)
    assert g(foo=True) == [1, 2.0]


def test_list_output_annotations(f: Callable[..., Any]) -> None:
    g = list_output(f, set_annotations=True)
    assert inspect.get_annotations(g) == {"foo": "bool", "return": ["int", "float"]}


def test_aggregated_output_decorator(f: Callable[..., Any]) -> None:
    g = aggregated_output(f, aggregator=lambda x, y: x + y)
    assert g(foo=True) == 3


def test_single_output_direct_call(f: Callable[..., Any]) -> None:
    g = single_output(f)
    assert g(foo=True) == 1


def test_single_output_decorator() -> None:
    @single_output
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


def test_dict_output_keys_none() -> None:
    with pytest.raises(DagsError, match="'keys' parameter is required"):
        dict_output(keys=None)  # ty: ignore[invalid-argument-type]


def test_aggregated_output_aggregator_none() -> None:
    with pytest.raises(DagsError, match="'aggregator' parameter is required"):
        aggregated_output(aggregator=None)  # ty: ignore[invalid-argument-type]


def test_aggregated_output_direct_call() -> None:
    def f():
        return (10, 20)

    g = aggregated_output(f, aggregator=lambda x, y: x + y)
    assert g() == 30


def test_aggregated_output_decorator_usage() -> None:
    @aggregated_output(aggregator=lambda x, y: x + y)
    def f():
        return (10, 20)

    assert f() == 30
