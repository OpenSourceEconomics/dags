import inspect
from typing import TypedDict, get_type_hints

from dags.output import aggregated_output, dict_output, list_output, single_output


def test_single_output_decorator() -> None:
    @single_output
    def f() -> tuple[int, ...]:
        return (1,)

    assert f() == 1

    def expected() -> int:
        return 1

    assert inspect.signature(f) == inspect.signature(expected)


def test_dict_output_decorator() -> None:
    @dict_output(keys=["a", "b"])
    def f() -> tuple[int, float]:
        return (1, 2.0)

    assert f() == {"a": 1, "b": 2.0}

    class FReturn(TypedDict):
        a: int
        b: float

    def expected() -> FReturn:
        return {"a": 1, "b": 2.0}

    got_signature = inspect.signature(f)
    expected_signature = inspect.signature(expected)

    assert got_signature.parameters == expected_signature.parameters
    # In the "dict" case, the return annotation is a TypedDict. This cannot be compared
    # using ==, so we compare the name and implied dictionary of type hints.
    assert (
        got_signature.return_annotation.__name__
        == expected_signature.return_annotation.__name__
    )
    assert get_type_hints(got_signature.return_annotation) == get_type_hints(
        expected_signature.return_annotation
    )


def test_list_output_decorator() -> None:
    @list_output
    def f() -> tuple[int, float]:
        return (1, 2.0)

    assert f() == [1, 2.0]

    def expected() -> list[int | float]:
        return [1, 2.0]

    assert inspect.signature(f) == inspect.signature(expected)


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
