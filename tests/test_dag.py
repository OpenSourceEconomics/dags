from __future__ import annotations

import inspect
from functools import partial
from typing import TYPE_CHECKING, Any, Literal

import pytest

from dags.annotations import _get_str_repr
from dags.dag import (
    DagsWarning,
    FunctionExecutionInfo,
    concatenate_functions,
    create_dag,
    get_ancestors,
    get_annotations,
)
from dags.exceptions import CyclicDependencyError, DagsError

if TYPE_CHECKING:
    from collections.abc import Callable


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
def concatenated_no_target() -> Callable[..., Any]:
    return concatenate_functions(
        functions=[_utility, _leisure, _consumption], set_annotations=True
    )


@pytest.fixture
def concatenated_utility_target() -> Callable[..., Any]:
    return concatenate_functions(
        functions=[_utility, _unrelated, _leisure, _consumption],
        targets="_utility",
        set_annotations=True,
    )


def test_concatenate_functions_with_dag(
    concatenated_no_target: Callable[..., Any],
) -> None:
    dag = create_dag(
        functions=[_utility, _unrelated, _leisure, _consumption], targets="_utility"
    )
    concatenated = concatenate_functions(
        functions=[_utility, _leisure, _consumption],
        dag=dag,
        set_annotations=True,
    )
    assert inspect.signature(concatenated) == inspect.signature(concatenated_no_target)


def test_concatenate_functions_no_target_results(
    concatenated_no_target: Callable[..., Any],
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


def test_concatenate_functions_no_target_annotations(
    concatenated_no_target: Callable[..., Any],
) -> None:
    expected_annotations = {
        "working_hours": "int",
        "wage": "float",
        "leisure_weight": "float",
        "return": ("float", "int", "float"),
    }
    assert inspect.get_annotations(concatenated_no_target) == expected_annotations


def test_concatenate_functions_single_target_results(
    concatenated_utility_target: Callable[..., Any],
) -> None:
    calculated_result = concatenated_utility_target(
        wage=5, working_hours=8, leisure_weight=2
    )
    expected_result = _complete_utility(wage=5, working_hours=8, leisure_weight=2)

    assert calculated_result == expected_result


def test_concatenate_functions_single_target_annotations(
    concatenated_utility_target: Callable[..., Any],
) -> None:
    expected_annotations = {
        "working_hours": "int",
        "wage": "float",
        "leisure_weight": "float",
        "return": "float",
    }
    assert inspect.get_annotations(concatenated_utility_target) == expected_annotations


@pytest.mark.parametrize("return_type", ["tuple", "list", "dict"])
def test_concatenate_functions_multi_target_result(
    return_type: Literal["tuple", "list", "dict"],
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
def test_concatenate_functions_multi_target_annotations(
    return_type: Literal["tuple", "list", "dict"],
) -> None:
    concatenated = concatenate_functions(
        functions=[_utility, _unrelated, _leisure, _consumption],
        targets=["_utility", "_consumption"],
        return_type=return_type,
        set_annotations=True,
    )

    return_annotations = {
        "tuple": ("float", "float"),
        "list": ["float", "float"],
        "dict": {"_utility": "float", "_consumption": "float"},
    }
    expected_annotation = {
        "working_hours": "int",
        "wage": "float",
        "leisure_weight": "float",
        "return": return_annotations[return_type],
    }
    assert inspect.get_annotations(concatenated) == expected_annotation


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

    assert concatenated(c=True, d=4) == 1 + 2 + float(True) + 4
    assert inspect.get_annotations(concatenated) == {
        "c": "bool",
        "d": "int",
        "return": "float",
    }


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
def test_fail_if_cycle_in_dag(
    funcs: dict[str, Callable[..., Any]] | list[Callable[..., Any]],
) -> None:
    with pytest.raises(CyclicDependencyError):
        create_dag(functions=funcs, targets=["_utility"])


def test_get_annotations() -> None:
    def f(a: int, b: float) -> float:
        return 1.0

    got = get_annotations(f, eval_str=False)
    exp = {"a": "int", "b": "float", "return": "float"}
    assert got == exp


def test_get_annotations_partial() -> None:
    def f(a: int, b: float) -> float:
        return 1.0

    partial_f = partial(f, a=1)
    got = get_annotations(partial_f, eval_str=False)
    exp = {"b": "float", "return": "float"}
    assert got == exp


def test_get_str_repr() -> None:
    assert _get_str_repr("int") == "int"
    assert _get_str_repr(int) == "int"
    assert _get_str_repr(1) == "1"


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
        "a": "no_annotation_found",
        "return": ("no_annotation_found",),
    }


# ======================================================================================
# Tests for aggregator return type inference
# ======================================================================================


def test_aggregator_return_type_explicit() -> None:
    """Test that explicit aggregator_return_type is used."""

    def f1() -> bool:
        return True

    def f2() -> bool:
        return False

    aggregated = concatenate_functions(
        functions={"f1": f1, "f2": f2},
        targets=["f1", "f2"],
        aggregator=lambda a, b: a and b,
        aggregator_return_type="bool",
        set_annotations=True,
    )

    assert aggregated() is False
    assert inspect.get_annotations(aggregated)["return"] == "bool"


def test_aggregator_return_type_inferred_from_targets() -> None:
    """Test that return type is inferred from targets when all have same type."""

    def f1() -> bool:
        return True

    def f2() -> bool:
        return False

    aggregated = concatenate_functions(
        functions={"f1": f1, "f2": f2},
        targets=["f1", "f2"],
        aggregator=lambda a, b: a and b,
        set_annotations=True,
    )

    assert aggregated() is False
    # Return type should be inferred from targets (both bool)
    assert inspect.get_annotations(aggregated)["return"] == "bool"


def test_aggregator_return_type_inferred_from_typed_aggregator() -> None:
    """Test that return type is inferred from aggregator annotations."""

    def f1():
        return True

    def f2():
        return False

    def typed_and(a: bool, b: bool) -> bool:
        return a and b

    aggregated = concatenate_functions(
        functions={"f1": f1, "f2": f2},
        targets=["f1", "f2"],
        aggregator=typed_and,
        set_annotations=True,
    )

    assert aggregated() is False
    # Return type should be inferred from aggregator
    assert inspect.get_annotations(aggregated)["return"] == "bool"


def test_aggregator_return_type_warns_when_cannot_infer() -> None:
    """Test that a warning is issued when return type cannot be inferred."""

    def f1():
        return True

    def f2():
        return False

    with pytest.warns(DagsWarning, match="Consider providing aggregator_return_type"):
        aggregated = concatenate_functions(
            functions={"f1": f1, "f2": f2},
            targets=["f1", "f2"],
            aggregator=lambda a, b: a and b,
            set_annotations=True,
        )

    assert aggregated() is False


def test_aggregator_return_type_mixed_target_types_uses_aggregator() -> None:
    """Test inference when targets have different types but aggregator is typed."""

    def f1() -> int:
        return 1

    def f2() -> float:
        return 2.0

    def typed_add(a: float, b: float) -> float:
        return a + b

    aggregated = concatenate_functions(
        functions={"f1": f1, "f2": f2},
        targets=["f1", "f2"],
        aggregator=typed_add,
        set_annotations=True,
    )

    assert aggregated() == 3.0
    # Return type should be inferred from aggregator since target types differ
    assert inspect.get_annotations(aggregated)["return"] == "float"
