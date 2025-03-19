"""Tests for parameters handling in dag_tree."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from dags.dag_tree.dag_tree import (
    _create_parameter_name_mapper,
    _link_parameter_to_function_or_input,
    _map_parameter,
)

if TYPE_CHECKING:
    from dags.dag_tree.typing import (
        GenericCallable,
        GlobalOrLocal,
        NestedFunctionDict,
        NestedInputStructureDict,
    )


def f(g, namespace1__f1, global_input, namespace1__input):
    """Global function, duplicate simple name."""
    return {
        "name": "f",
        "args": {
            "g": g,
            "namespace1__f1": namespace1__f1,
            "global_input": global_input,
            "namespace1__input": namespace1__input,
        },
    }


def g():
    """Global function, unique simple name."""
    return {"name": "g"}


def _namespace1__f(
    g,
    namespace1__f1,
    namespace2__f2,
    namespace1__input,
    namespace2__input2,
):
    """Namespaced function, duplicate simple name."""
    return {
        "name": "namespace1__f",
        "args": {
            "g": g,
            "namespace1__f1": namespace1__f1,
            "namespace2__f2": namespace2__f2,
            "namespace1__input": namespace1__input,
            "namespace2__input2": namespace2__input2,
        },
    }


def _namespace1__f1():
    """Namespaced function, unique simple name."""
    return {"name": "namespace1__f1"}


def _namespace2__f(f2, global_input):
    """Namespaced function, duplicate simple name. All arguments with simple names."""
    return {"name": "namespace2__f", "args": {"f2": f2, "global_input": global_input}}


def _namespace2__f2():
    """Namespaced function, unique simple name."""
    return {"name": "namespace2__f2"}


def _namespace1__deep__f():
    """A deeply nested function."""
    return {"name": "namespace1_deep__f"}


@pytest.fixture
def functions() -> NestedFunctionDict:
    return {
        "f": f,
        "g": g,
        "namespace1": {
            "f": _namespace1__f,
            "f1": _namespace1__f1,
            "deep": {"f": _namespace1__deep__f},
        },
        "namespace2": {
            "f": _namespace2__f,
            "f2": _namespace2__f2,
        },
    }


@pytest.mark.parametrize(
    ("input_structure", "namespace", "function", "expected"),
    [
        (
            {},
            "",
            g,
            {},
        ),
        (
            {"global_input": None},
            "namespace2",
            _namespace2__f,
            {"f2": "namespace2__f2", "global_input": "global_input"},
        ),
        (
            {"namespace2": {"global_input": None}},
            "namespace2",
            _namespace2__f,
            {"f2": "namespace2__f2", "global_input": "namespace2__global_input"},
        ),
        (
            {"namespace1": {"input": None}, "namespace2": {"input2": None}},
            "namespace1",
            _namespace1__f,
            {
                "g": "g",
                "namespace1__f1": "namespace1__f1",
                "namespace2__f2": "namespace2__f2",
                "namespace1__input": "namespace1__input",
                "namespace2__input2": "namespace2__input2",
            },
        ),
        (
            {"global_input": None, "namespace1": {"global_input": None}},
            "",
            f,
            {
                "g": "g",
                "namespace1__f1": "namespace1__f1",
                "global_input": "global_input",
                "namespace1__input": "namespace1__input",
            },
        ),
    ],
)
def test_create_parameter_name_mapper(
    functions: NestedFunctionDict,
    input_structure: NestedInputStructureDict,
    namespace: str,
    function: GenericCallable,
    expected: dict[str, str],
) -> None:
    from dags.dag_tree.tree_utils import flatten_to_qual_names

    flat_functions = flatten_to_qual_names(functions)
    flat_input_structure = flatten_to_qual_names(input_structure)

    assert (
        _create_parameter_name_mapper(
            flat_functions,
            flat_input_structure,
            namespace,
            function,
        )
        == expected
    )


def test_map_parameter_raises() -> None:
    with pytest.raises(ValueError, match="Cannot resolve parameter"):
        _map_parameter({}, {}, "x", "x")


@pytest.mark.parametrize(
    ("level_of_inputs", "namespace", "parameter_name", "expected"),
    [
        ("local", "namespace1", "namespace1__f1", "namespace1__f1"),
        ("local", "namespace1", "f1", "namespace1__f1"),
        (
            "local",
            "namespace1",
            "g",
            "g",
        ),
        ("local", "namespace1", "input", "namespace1__input"),
        ("local", "", "input", "input"),
        ("global", "namespace1", "input", "input"),
        ("global", "", "input", "input"),
    ],
)
def test_link_parameter_to_function_or_input(
    functions: NestedFunctionDict,
    level_of_inputs: GlobalOrLocal,
    namespace: str,
    parameter_name: str,
    expected: tuple[str],
) -> None:
    from dags.dag_tree.tree_utils import flatten_to_qual_names

    flat_functions = flatten_to_qual_names(functions)
    assert (
        _link_parameter_to_function_or_input(
            flat_functions,
            namespace,
            parameter_name,
            level_of_inputs,
        )
        == expected
    )
