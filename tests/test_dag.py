import inspect
from functools import partial

import pytest

from dags.dag import (
    concatenate_functions,
    create_dag,
    get_ancestors,
)
from dags.dags_typing import (
    CombinedFunctionReturnType,
    FunctionCollection,
)


def _utility(_consumption, _leisure, leisure_weight):
    return _consumption + leisure_weight * _leisure


def _leisure(working_hours):
    return 24 - working_hours


def _consumption(working_hours, wage):
    return wage * working_hours


def _unrelated(working_hours):
    msg = "This should not be called."
    raise NotImplementedError(msg)


def _leisure_cycle(working_hours, _utility):
    return 24 - working_hours + _utility


def _consumption_cycle(working_hours, wage, _utility):
    return wage * working_hours + _utility


def _complete_utility(wage, working_hours, leisure_weight):
    """The function that we try to generate dynamically."""
    leis = _leisure(working_hours)
    cons = _consumption(working_hours, wage)
    return _utility(cons, leis, leisure_weight)


def test_concatenate_functions_no_target() -> None:
    concatenated = concatenate_functions(functions=[_utility, _leisure, _consumption])

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


def test_concatenate_functions_single_target() -> None:
    concatenated = concatenate_functions(
        functions=[_utility, _unrelated, _leisure, _consumption],
        targets="_utility",
    )

    calculated_result = concatenated(wage=5, working_hours=8, leisure_weight=2)

    expected_result = _complete_utility(wage=5, working_hours=8, leisure_weight=2)
    assert calculated_result == expected_result

    calculated_args = set(inspect.signature(concatenated).parameters)
    expected_args = {"leisure_weight", "wage", "working_hours"}

    assert calculated_args == expected_args


@pytest.mark.parametrize("return_type", ["tuple", "list", "dict"])
def test_concatenate_functions_multi_target(
    return_type: CombinedFunctionReturnType,
) -> None:
    concatenated = concatenate_functions(
        functions=[_utility, _unrelated, _leisure, _consumption],
        targets=["_utility", "_consumption"],
        return_type=return_type,
    )

    calculated_result = concatenated(wage=5, working_hours=8, leisure_weight=2)

    expected_result = {
        "_utility": _complete_utility(wage=5, working_hours=8, leisure_weight=2),
        "_consumption": _consumption(wage=5, working_hours=8),
    }
    if return_type == "tuple":
        assert calculated_result == tuple(expected_result.values())
    elif return_type == "list":
        assert calculated_result == list(expected_result.values())
    else:
        assert calculated_result == expected_result

    calculated_args = set(inspect.signature(concatenated).parameters)
    expected_args = {"leisure_weight", "wage", "working_hours"}

    assert calculated_args == expected_args


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
    def f(a, b, c):
        return a + b + c

    def g(f, d):
        return f + d

    concatenated = concatenate_functions(
        functions={"f": partial(f, 1, b=2), "g": g},
        targets="g",
    )

    assert list(inspect.signature(concatenated).parameters) == ["c", "d"]
    assert concatenated(3, 4) == 10


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
