import inspect

from dags.output import aggregated_output, dict_output, list_output, single_output


def test_single_output_decorator() -> None:
    @single_output
    def f() -> tuple[int, ...]:
        return (1,)

    assert f() == 1

    def expected_signature() -> int:
        return 1

    assert inspect.signature(f) == inspect.signature(expected_signature)


def test_dict_output_decorator() -> None:
    @dict_output(keys=["a", "b"])
    def f() -> tuple[int, float]:
        return (1, 2.0)

    assert f() == {"a": 1, "b": 2.0}

    def expected_signature() -> dict[str, int | float]:
        return {"a": 1, "b": 2.0}

    assert inspect.signature(f) == inspect.signature(expected_signature)


def test_list_output_decorator() -> None:
    @list_output
    def f() -> tuple[int, float]:
        return (1, 2.0)

    assert f() == [1, 2.0]

    def expected_signature() -> list[int | float]:
        return [1, 2.0]

    assert inspect.signature(f) == inspect.signature(expected_signature)


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
