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

# Import fixtures
from .conftest import (
    _global__f,
    _global__g,
    _namespace1__f,
    _namespace2__f,
)


@pytest.mark.parametrize(
    ("input_structure", "namespace", "function", "expected"),
    [
        (
            {},
            "",
            _global__g,
            {},
        ),
        (
            {"input_": None},
            "namespace2",
            _namespace2__f,
            {"f2": "namespace2__f2", "input_": "input_"},
        ),
        (
            {"namespace2": {"input_": None}},
            "namespace2",
            _namespace2__f,
            {"f2": "namespace2__f2", "input_": "namespace2__input_"},
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
            {"input_": None, "namespace1": {"input_": None}},
            "",
            _global__f,
            {
                "g": "g",
                "namespace1__f1": "namespace1__f1",
                "input_": "input_",
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
