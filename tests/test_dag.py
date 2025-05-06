import inspect
from functools import partial

import pytest

from dags.dag import (
    concatenate_functions,
    create_dag,
    get_ancestors,
)
from dags.typing import (
    CombinedFunctionReturnType,
    FunctionCollection,
    GenericCallable,
)


def _utility(_consumption: float, _leisure: int, leisure_weight: float) -> float:
    return _consumption + leisure_weight * _leisure


def _leisure(working_hours: int) -> int:
    return 24 - working_hours


def _consumption(working_hours: int, wage: float) -> float:
    return wage * working_hours


def _unrelated(working_hours: int) -> None:
    msg = "This should not be called."
    raise NotImplementedError(msg)


def _leisure_cycle(working_hours: int, _utility: float) -> float:
    return 24 - working_hours + _utility


def _consumption_cycle(working_hours: int, wage: float, _utility: float) -> float:
    return wage * working_hours + _utility


def _complete_utility(wage: float, working_hours: int, leisure_weight: float) -> float:
    """The function that we try to generate dynamically."""
    leis = _leisure(working_hours)
    cons = _consumption(working_hours, wage)
    return _utility(cons, leis, leisure_weight)


@pytest.fixture
def concatenated_no_target() -> GenericCallable:
    return concatenate_functions(
        functions=[_utility, _leisure, _consumption], set_annotations=True
    )


@pytest.fixture
def concatenated_utility_target() -> GenericCallable:
    return concatenate_functions(
        functions=[_utility, _unrelated, _leisure, _consumption],
        targets="_utility",
        set_annotations=True,
    )


def test_concatenate_functions_no_target_results(
    concatenated_no_target: GenericCallable,
) -> None:
    calculated_result = concatenated_no_target(
        wage=5, working_hours=8, leisure_weight=2
    )

    expected_utility = _complete_utility(wage=5, working_hours=8, leisure_weight=2)
    expected_leisure = _leisure(working_hours=8)
    expected_consumption = _consumption(working_hours=8, wage=5)

    assert calculated_result == (
        expected_utility,
        expected_leisure,
        expected_consumption,
    )


def test_concatenate_functions_no_target_signature(
    concatenated_no_target: GenericCallable,
) -> None:
    def expected(  # type: ignore[empty-body]
        working_hours: int,
        wage: float,
        leisure_weight: float,
    ) -> tuple[float, int, float]:
        pass

    assert inspect.signature(concatenated_no_target) == inspect.signature(expected)


def test_concatenate_functions_single_target_results(
    concatenated_utility_target: GenericCallable,
) -> None:
    calculated_result = concatenated_utility_target(
        wage=5, working_hours=8, leisure_weight=2
    )
    expected_result = _complete_utility(wage=5, working_hours=8, leisure_weight=2)

    assert calculated_result == expected_result


def test_concatenate_functions_single_target_signature(
    concatenated_utility_target: GenericCallable,
) -> None:
    def expected(working_hours: int, wage: float, leisure_weight: float) -> float:  # type: ignore[empty-body]
        pass

    assert inspect.signature(concatenated_utility_target) == inspect.signature(expected)


@pytest.mark.parametrize("return_type", ["tuple", "list", "dict"])
def test_concatenate_functions_multi_target(
    return_type: CombinedFunctionReturnType,
) -> None:
    concatenated = concatenate_functions(
        functions=[_utility, _unrelated, _leisure, _consumption],
        targets=["_utility", "_consumption"],
        return_type=return_type,
        set_annotations=True,
    )

    calculated_result = concatenated(wage=5, working_hours=8, leisure_weight=2)

    _expected_result = {
        "_utility": _complete_utility(wage=5, working_hours=8, leisure_weight=2),
        "_consumption": _consumption(wage=5, working_hours=8),
    }

    return_annotation: type[object]
    expected_result: tuple[float, ...] | list[float] | dict[str, float]
    if return_type == "tuple":
        return_annotation = tuple[float, float]
        expected_result = tuple(_expected_result.values())
    elif return_type == "list":
        return_annotation = list[float]
        expected_result = list(_expected_result.values())
    elif return_type == "dict":
        return_annotation = dict[str, float]
        expected_result = dict(_expected_result)

    def expected(
        working_hours: int, wage: float, leisure_weight: float
    ) -> return_annotation:  # type: ignore[valid-type]
        pass

    assert calculated_result == expected_result
    assert inspect.signature(concatenated) == inspect.signature(expected)


def test_get_ancestors_many_ancestors() -> None:
    calculated = get_ancestors(
        functions=[_utility, _unrelated, _leisure, _consumption],
        targets="_utility",
    )
    expected = {"_consumption", "_leisure", "working_hours", "wage", "leisure_weight"}

    assert calculated == expected


def test_get_ancestors_few_ancestors() -> None:
    calculated = get_ancestors(
        functions=[_utility, _unrelated, _leisure, _consumption],
        targets="_unrelated",
    )

    expected = {"working_hours"}

    assert calculated == expected


def test_get_ancestors_multiple_targets() -> None:
    calculated = get_ancestors(
        functions=[_utility, _unrelated, _leisure, _consumption],
        targets=["_unrelated", "_consumption"],
    )

    expected = {"wage", "working_hours"}
    assert calculated == expected


def test_concatenate_functions_with_aggregation_via_and() -> None:
    funcs = {"f1": lambda: True, "f2": lambda: False}
    aggregated = concatenate_functions(
        functions=funcs,
        targets=["f1", "f2"],
        aggregator=lambda a, b: a and b,
    )
    assert not aggregated()


def test_concatenate_functions_with_aggregation_via_or() -> None:
    funcs = {"f1": lambda: True, "f2": lambda: False}
    aggregated = concatenate_functions(
        functions=funcs,
        targets=["f1", "f2"],
        aggregator=lambda a, b: a or b,
    )
    assert aggregated()


def test_partialled_argument_is_ignored() -> None:
    def f(a: float, b: int, c: bool) -> float:
        return a + b + float(c)

    def g(f: float, d: int) -> float:
        return f + d

    concatenated = concatenate_functions(
        functions={"f": partial(f, 1, b=2), "g": g},
        targets="g",
        set_annotations=True,
    )

    def expected(c: bool, d: int) -> float:
        return 1 + 2 + float(c) + d

    assert concatenated(c=True, d=4) == expected(c=True, d=4)
    assert inspect.signature(concatenated) == inspect.signature(expected)


@pytest.mark.parametrize(
    "funcs",
    [
        {
            "_utility": _utility,
            "_leisure": _leisure,
            "_consumption": _consumption_cycle,
        },
        {
            "_utility": _utility,
            "_leisure": _leisure_cycle,
            "_consumption": _consumption_cycle,
        },
    ],
)
def test_fail_if_cycle_in_dag(funcs: FunctionCollection) -> None:
    with pytest.raises(
        ValueError,
        match="The DAG contains one or more cycles:",
    ):
        create_dag(
            functions=funcs,
            targets=["_utility"],
        )
