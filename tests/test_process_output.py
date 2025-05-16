from __future__ import annotations

import inspect

from dags.output import (
    _create_dict_return_annotation,
    _create_list_return_annotation,
    aggregated_output,
    dict_output,
    list_output,
    single_output,
)


def test_single_output() -> None:
    @single_output
    def f() -> tuple[int, ...]:
        return (1,)

    assert f() == 1


def test_single_output_annotations() -> None:
    def _f(foo: bool) -> tuple[int, ...]:
        return (int(foo),)

    f = single_output(_f, set_annotations=True)

    assert inspect.get_annotations(f, eval_str=True) == {
        "foo": bool,
        "return": int,
    }


def test_dict_output() -> None:
    @dict_output(keys=["a", "b"])
    def f() -> tuple[int, float]:
        return (1, 2.0)

    assert f() == {"a": 1, "b": 2.0}


def test_dict_output_annotations() -> None:
    @dict_output(keys=["a", "b"], set_annotations=True)
    def f(foo: bool) -> tuple[int, float]:
        return (int(foo), 2.0)

    assert inspect.get_annotations(f, eval_str=True) == {
        "foo": bool,
        "return": {"a": int, "b": float},
    }


def test_list_output() -> None:
    @list_output
    def f() -> tuple[int, float]:
        return (1, 2.0)

    assert f() == [1, 2.0]


def test_list_output_annotations() -> None:
    def _f(foo: bool) -> tuple[int, float]:
        return (int(foo), 2.0)

    f = list_output(_f, set_annotations=True)

    assert inspect.get_annotations(f, eval_str=True) == {
        "foo": bool,
        "return": [int, float],
    }


def test_aggregated_output_decorator() -> None:
    @aggregated_output(aggregator=lambda x, y: x + y)
    def f():
        return (1, 2)

    assert f() == 3


def test_single_output_direct_call() -> None:
    def f() -> tuple[int, ...]:
        return (1,)

    g = single_output(f)

    assert g() == 1


def test_dict_output_direct_call() -> None:
    def f():
        return (1, 2)

    g = dict_output(f, keys=["a", "b"])

    assert g() == {"a": 1, "b": 2}


def test_list_output_direct_call() -> None:
    def f():
        return (1, 2)

    g = list_output(f)

    assert g() == [1, 2]


def test_aggregated_output_direct_call() -> None:
    def f():
        return (1, 2)

    g = aggregated_output(f, aggregator=lambda x, y: x + y)
    assert g() == 3


def test_create_dict_return_annotation() -> None:
    keys = ["a", "b"]
    tuple_of_types = ("int", float)
    assert (
        _create_dict_return_annotation(keys, tuple_of_types) == "{'a': int, 'b': float}"
    )


def test_create_list_return_annotation() -> None:
    tuple_of_types = ("int", float)
    assert _create_list_return_annotation(tuple_of_types) == "[int, float]"
