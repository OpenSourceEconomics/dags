"""Tests for the dag_tree module."""

from __future__ import annotations

import functools
import inspect
from typing import TYPE_CHECKING

import pytest

from dags.tree import (
    concatenate_functions_tree,
    create_input_structure_tree,
)
from dags.tree.dag_tree import (
    _qual_name_functions_only_abs_paths,
)

if TYPE_CHECKING:
    from dags.tree.typing import (
        NestedFunctionDict,
        NestedInputDict,
        NestedInputStructureDict,
        NestedOutputDict,
        NestedTargetDict,
    )


def f(g, a, b):
    return g, a, b


def g(a):
    return a


def h(a, b__g):
    return a, b__g


@pytest.fixture
def functions_simple() -> NestedFunctionDict:
    return {
        "n1": {
            "f": f,
            "g": g,
        },
        "n2": {
            "h": h,
        },
    }


@pytest.fixture
def functions_nested_and_duplicate_g() -> NestedFunctionDict:
    return {
        "n1": {
            "f": f,
            "g": g,
        },
        "n2": {"h": h, "b": {"g": g}},
    }


@pytest.mark.parametrize(
    ("targets", "top_level_inputs", "expected"),
    [
        (
            None,
            set(),
            {
                "n1": {
                    "a": None,
                    "b": None,
                },
                "n2": {
                    "a": None,
                    "b": {"g": None},
                },
            },
        ),
        (
            None,
            {"a"},
            {
                "a": None,
                "n1": {
                    "b": None,
                },
                "n2": {
                    "b": {"g": None},
                },
            },
        ),
        (
            {"n1": {"f": None}},
            set(),
            {
                "n1": {
                    "a": None,
                    "b": None,
                },
            },
        ),
        (
            {"n1": {"f": None}},
            {"a"},
            {
                "a": None,
                "n1": {
                    "b": None,
                },
            },
        ),
        (
            None,
            {"a", "g"},
            {"raises_error": None},
        ),
    ],
)
def test_create_input_structure_tree_simple(
    functions_simple: NestedFunctionDict,
    targets: NestedTargetDict | None,
    top_level_inputs: set[str],
    expected: NestedInputStructureDict,
) -> None:
    if "raises_error" in expected:
        with pytest.raises(
            ValueError, match="Elements of the top-level namespace must not be repeated"
        ):
            create_input_structure_tree(functions_simple, targets, top_level_inputs)
    else:
        assert (
            create_input_structure_tree(functions_simple, targets, top_level_inputs)
            == expected
        )


@pytest.mark.parametrize(
    ("targets", "top_level_inputs", "expected"),
    [
        (
            None,
            set(),
            {
                "n1": {
                    "a": None,
                    "b": None,
                },
                "n2": {
                    "a": None,
                    "b": {"a": None},
                },
            },
        ),
        (
            None,
            {"a"},
            {
                "a": None,
                "n1": {
                    "b": None,
                },
            },
        ),
        (
            None,
            {"a", "g"},
            {"raises_error": None},
        ),
    ],
)
def test_create_input_structure_tree_nested_and_duplicate_g(
    functions_nested_and_duplicate_g: NestedFunctionDict,
    targets: NestedTargetDict | None,
    top_level_inputs: set[str],
    expected: NestedInputStructureDict,
) -> None:
    if "raises_error" in expected:
        with pytest.raises(
            ValueError, match="Elements of the top-level namespace must not be repeated"
        ):
            create_input_structure_tree(
                functions_nested_and_duplicate_g, targets, top_level_inputs
            )
    else:
        assert (
            create_input_structure_tree(
                functions_nested_and_duplicate_g, targets, top_level_inputs
            )
            == expected
        )


def test_create_input_structure_tree_duplicates_lower_in_hierarchy() -> None:
    assert create_input_structure_tree(
        functions={
            "n1": {"a": {"f": f}},
        },
        targets=None,
        top_level_inputs=set(),
    ) == {"n1": {"a": {"a": None, "b": None, "g": None}}}


@pytest.mark.parametrize(
    ("targets", "global_input", "expected"),
    [
        (
            None,
            {
                "global_input": "namespace1__input",
                "namespace1": {
                    "input": "namespace1__input",
                },
                "namespace2": {
                    "global_input": "namespace2__input",
                    "input2": "namespace2__input2",
                },
            },
            {
                "f": {
                    "name": "f",
                    "args": {
                        "g": {"name": "g"},
                        "namespace1__f1": {"name": "namespace1__f1"},
                        "global_input": "namespace1__input",
                        "namespace1__input": "namespace1__input",
                    },
                },
                "g": {"name": "g"},
                "namespace1": {
                    "f": {
                        "name": "namespace1__f",
                        "args": {
                            "g": {"name": "g"},
                            "namespace1__f1": {"name": "namespace1__f1"},
                            "namespace2__f2": {"name": "namespace2__f2"},
                            "namespace1__input": "namespace1__input",
                            "namespace2__input2": "namespace2__input2",
                        },
                    },
                    "f1": {"name": "namespace1__f1"},
                    "deep": {
                        "f": {"name": "namespace1_deep__f"},
                    },
                },
                "namespace2": {
                    "f": {
                        "name": "namespace2__f",
                        "args": {
                            "f2": {"name": "namespace2__f2"},
                            "global_input": "namespace2__input",
                        },
                    },
                    "f2": {"name": "namespace2__f2"},
                },
            },
        ),
        (
            {
                "namespace1": {
                    "f": None,
                },
                "namespace2": {"f": None},
            },
            {
                "global_input": "global__input",
                "namespace1": {
                    "input": "namespace1__input",
                },
                "namespace2": {
                    "input2": "namespace2__input2",
                },
            },
            {
                "namespace1": {
                    "f": {
                        "name": "namespace1__f",
                        "args": {
                            "g": {"name": "g"},
                            "namespace1__f1": {"name": "namespace1__f1"},
                            "namespace2__f2": {"name": "namespace2__f2"},
                            "namespace1__input": "namespace1__input",
                            "namespace2__input2": "namespace2__input2",
                        },
                    },
                },
                "namespace2": {
                    "f": {
                        "name": "namespace2__f",
                        "args": {
                            "f2": {"name": "namespace2__f2"},
                            "global_input": "global__input",
                        },
                    },
                },
            },
        ),
    ],
)
def test_concatenate_functions_tree(
    functions: NestedFunctionDict,
    targets: NestedTargetDict,
    global_input: NestedInputDict,
    expected: NestedOutputDict,
) -> None:
    f = concatenate_functions_tree(
        functions, targets, global_input, name_clashes="ignore"
    )
    assert f(global_input) == expected


def test_partialled_function_argument() -> None:
    def f(a, b):
        return a + b

    f_partial = functools.partial(f, b=1)
    tree = {"f": f_partial}
    input_structure = {"a": None}
    targets = {"f": None}

    concatenated_func = concatenate_functions_tree(tree, targets, input_structure)
    concatenated_func({"a": 1})


@pytest.mark.parametrize(
    (
        "functions_tree",
        "input_structure",
        "qual_name_function_name_to_check",
        "expected_argument_name",
    ),
    [
        (
            {
                "common_name": {"foo": lambda x: x},
                "target_namespace": {
                    "common_name": {"foo": lambda x: x},
                    "target_leaf": lambda common_name__foo: common_name__foo,
                },
            },
            {
                "common_name": {"x": None},
                "target_namespace": {"common_name": {"x": None}},
            },
            "target_namespace__target_leaf",
            "common_name__foo",
        ),
        (
            {
                "common_name": {"föö": lambda x: x},
                "target_namespace": {
                    "common_name": {"föö": lambda x: x},
                    "target_leaf": lambda common_name__föö: common_name__föö,
                },
            },
            {
                "common_name": {"x": None},
                "target_namespace": {"common_name": {"x": None}},
            },
            "target_namespace__target_leaf",
            "common_name__föö",
        ),
    ],
)
def test_correct_argument_names(
    functions_tree: NestedFunctionDict,
    input_structure: NestedInputStructureDict,
    qual_name_function_name_to_check: str,
    expected_argument_name: str,
) -> None:
    qual_name_functions = _qual_name_functions_only_abs_paths(
        functions_tree, input_structure
    )
    assert (
        expected_argument_name
        in inspect.signature(
            qual_name_functions[qual_name_function_name_to_check]
        ).parameters
    )
