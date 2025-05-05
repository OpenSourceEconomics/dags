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


def test_concatenate_functions_no_target() -> None:
    concatenated = concatenate_functions(
        functions=[_utility, _leisure, _consumption], set_annotations=True
    )

    calculated_result = concatenated(wage=5, working_hours=8, leisure_weight=2)

    expected_utility = _complete_utility(wage=5, working_hours=8, leisure_weight=2)
    expected_leisure = _leisure(working_hours=8)
    expected_consumption = _consumption(working_hours=8, wage=5)

    assert calculated_result == (
        expected_utility,
        expected_leisure,
        expected_consumption,
    )

    calculated_args = set(inspect.signature(concatenated).parameters)
    expected_args = {"leisure_weight", "wage", "working_hours"}

    assert calculated_args == expected_args

    def expected(
        working_hours: int,  # noqa: ARG001
        wage: float,  # noqa: ARG001
        leisure_weight: float,  # noqa: ARG001
    ) -> tuple[float, int, float]:
        return (1.0, 1, 1.0)

    assert inspect.signature(concatenated) == inspect.signature(expected)


def test_concatenate_functions_single_target() -> None:
    concatenated = concatenate_functions(
        functions=[_utility, _unrelated, _leisure, _consumption],
        targets="_utility",
        set_annotations=True,
    )

    calculated_result = concatenated(wage=5, working_hours=8, leisure_weight=2)

    expected_result = _complete_utility(wage=5, working_hours=8, leisure_weight=2)
    assert calculated_result == expected_result

    calculated_args = set(inspect.signature(concatenated).parameters)
    expected_args = {"leisure_weight", "wage", "working_hours"}

    assert calculated_args == expected_args

    def expected(
        working_hours: int,  # noqa: ARG001
        wage: float,  # noqa: ARG001
        leisure_weight: float,  # noqa: ARG001
    ) -> float:
        return 1.0

    assert inspect.signature(concatenated) == inspect.signature(expected)


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

    expected_result = {
        "_utility": _complete_utility(wage=5, working_hours=8, leisure_weight=2),
        "_consumption": _consumption(wage=5, working_hours=8),
    }
    if return_type == "tuple":

        def expected(
            working_hours: int,  # noqa: ARG001
            wage: float,  # noqa: ARG001
            leisure_weight: float,  # noqa: ARG001
        ) -> tuple[float, float]:
            return (1.0, 1.0)

        expected_result = tuple(expected_result.values())
    elif return_type == "list":

        def expected(
            working_hours: int,  # noqa: ARG001
            wage: float,  # noqa: ARG001
            leisure_weight: float,  # noqa: ARG001
        ) -> list[float]:
            return [1.0, 1.0]

        expected_result = list(expected_result.values())
    elif return_type == "dict":

        def expected(
            working_hours: int,  # noqa: ARG001
            wage: float,  # noqa: ARG001
            leisure_weight: float,  # noqa: ARG001
        ) -> dict[str, float]:
            return {"_utility": 1.0, "_consumption": 1.0}

        expected_result = dict(expected_result)

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

    assert concatenated(3, 4) == expected(c=3, d=4)
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
