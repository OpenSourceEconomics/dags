from __future__ import annotations

import inspect
from functools import partial
from typing import TYPE_CHECKING

import pytest

from dags.annotations import get_str_repr
from dags.dag import (
    FunctionExecutionInfo,
    concatenate_functions,
    create_dag,
    get_ancestors,
    get_annotations,
)
from dags.exceptions import CyclicDependencyError, DagsError

if TYPE_CHECKING:
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


@pytest.mark.parametrize("eval_str", [True, False])
def test_concatenate_functions_no_target_annotations(
    concatenated_no_target: GenericCallable,
    eval_str: bool,
) -> None:
    def expected(  # type: ignore[empty-body]
        working_hours: int,
        wage: float,
        leisure_weight: float,
    ) -> tuple[float, int, float]:
        pass

    assert inspect.get_annotations(
        concatenated_no_target, eval_str=eval_str
    ) == inspect.get_annotations(expected, eval_str=eval_str)


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
def test_concatenate_functions_multi_target_result(
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
    expected_result: tuple[float, ...] | list[float] | dict[str, float]
    if return_type == "tuple":
        expected_result = tuple(_expected_result.values())
    elif return_type == "list":
        expected_result = list(_expected_result.values())
    elif return_type == "dict":
        expected_result = dict(_expected_result)

    assert calculated_result == expected_result


@pytest.mark.parametrize("return_type", ["tuple", "list", "dict"])
def test_concatenate_functions_multi_target_signature_and_annotations(
    return_type: CombinedFunctionReturnType,
) -> None:
    concatenated = concatenate_functions(
        functions=[_utility, _unrelated, _leisure, _consumption],
        targets=["_utility", "_consumption"],
        return_type=return_type,
        set_annotations=True,
    )

    expected_functions: dict[str, GenericCallable] = {}

    if return_type == "tuple":

        def expected_tuple(  # type: ignore[empty-body]
            working_hours: int, wage: float, leisure_weight: float
        ) -> tuple[float, float]:
            pass

        expected_functions["tuple"] = expected_tuple

    elif return_type == "list":

        def expected_list(
            working_hours: int, wage: float, leisure_weight: float
        ) -> [float, float]:  # type: ignore[valid-type]
            pass

        expected_functions["list"] = expected_list
    elif return_type == "dict":

        def expected_dict(  # type: ignore[empty-body]
            working_hours: int, wage: float, leisure_weight: float
        ) -> {"_utility": float, "_consumption": float}:  # type: ignore[misc]  # noqa: UP037
            pass

        expected_functions["dict"] = expected_dict

    assert inspect.signature(expected_functions[return_type]) == inspect.signature(
        concatenated
    )
    assert inspect.get_annotations(
        expected_functions[return_type], eval_str=True
    ) == inspect.get_annotations(concatenated, eval_str=True)


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
    with pytest.raises(CyclicDependencyError):
        create_dag(functions=funcs, targets=["_utility"])


def test_get_annotations() -> None:
    def f(a: int, b: float) -> float:
        return 1.0

    got = get_annotations(f, eval_str=False)
    exp = {"a": "int", "b": "float", "return": "float"}
    assert got == exp


def test_get_annotations_eval_str() -> None:
    def f(a: int, b: float) -> float:
        return 1.0

    got = get_annotations(f, eval_str=True)
    exp = {"a": int, "b": float, "return": float}
    assert got == exp


def test_get_annotations_partial() -> None:
    def f(a: int, b: float) -> float:
        return 1.0

    partial_f = partial(f, a=1)
    got = get_annotations(partial_f, eval_str=False)
    exp = {"b": "float", "return": "float"}
    assert got == exp


def test_get_annotations_partial_eval_str() -> None:
    def f(a: int, b: float) -> float:
        return 1.0

    partial_f = partial(f, a=1)
    got = get_annotations(partial_f, eval_str=True)
    exp = {"b": float, "return": float}
    assert got == exp


def test_get_str_repr() -> None:
    assert get_str_repr("int") == "int"
    assert get_str_repr(int) == "int"
    assert get_str_repr(1) == "1"


def test_concatenate_functions_with_aggregator_and_multiple_targets() -> None:
    with pytest.raises(DagsError):
        concatenate_functions(
            functions={},
            targets=["f1", "f2"],
            aggregator=lambda a, b: a + b,
            set_annotations=True,
        )


def test_function_execution_info() -> None:
    def f(a: int, b: float) -> float:
        return a + b

    info = FunctionExecutionInfo(
        name="f",
        func=f,
        verify_annotations=True,
    )

    assert info.name == "f"
    assert info.func == f
    assert info.verify_annotations is True
    assert info.annotations == {"a": "int", "b": "float", "return": "float"}
    assert info.argument_annotations == {"a": "int", "b": "float"}
    assert info.return_annotation == "float"


def test_concatenate_functions_no_annotations_set_annotations() -> None:
    def f(a):
        return a

    concatenated = concatenate_functions(
        functions=[f],
        targets=None,
        set_annotations=True,
    )

    assert inspect.get_annotations(concatenated) == {
        "a": "None",
        "return": "tuple['None']",
    }
    assert inspect.get_annotations(concatenated, eval_str=True) == {
        "a": None,
        "return": tuple["None"],
    }
