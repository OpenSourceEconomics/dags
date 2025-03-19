"""Tests for the dag_tree module."""

from __future__ import annotations

import functools
import inspect
from typing import TYPE_CHECKING

import pytest

from dags.dag_tree import (
    concatenate_functions_tree,
    create_input_structure_tree,
)
from dags.dag_tree.dag_tree import (
    _flatten_functions_and_rename_parameters,
    _flatten_targets_to_qual_names,
)

if TYPE_CHECKING:
    from dags.dag_tree.typing import (
        GlobalOrLocal,
        NestedFunctionDict,
        NestedInputDict,
        NestedInputStructureDict,
        NestedOutputDict,
        NestedTargetDict,
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
    ("targets", "level_of_inputs", "expected"),
    [
        (
            None,
            "local",
            {
                "global_input": None,
                "namespace1": {
                    "input": None,
                },
                "namespace2": {
                    "global_input": None,
                    "input2": None,
                },
            },
        ),
        (
            None,
            "global",
            {
                "global_input": None,
                "namespace1": {
                    "input": None,
                },
                "namespace2": {
                    "input2": None,
                },
            },
        ),
        (
            {"f": None, "namespace2": {"f": None}},
            "local",
            {
                "global_input": None,
                "namespace1": {
                    "input": None,
                },
                "namespace2": {
                    "global_input": None,
                },
            },
        ),
        (
            {"f": None, "namespace2": {"f": None}},
            "global",
            {
                "global_input": None,
                "namespace1": {
                    "input": None,
                },
            },
        ),
    ],
)
def test_create_input_structure_tree(
    functions: NestedFunctionDict,
    targets: NestedTargetDict | None,
    level_of_inputs: GlobalOrLocal,
    expected: NestedInputStructureDict,
) -> None:
    assert create_input_structure_tree(functions, targets, level_of_inputs) == expected


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


@pytest.mark.parametrize(
    "functions",
    [
        {"x_": {"x": lambda x: x}},
        {"x": {"x_": {"x": lambda x: x}}, "y": lambda y: y},
    ],
)
def test_fail_if_branches_have_trailing_underscores(
    functions: NestedFunctionDict,
) -> None:
    with pytest.raises(
        ValueError, match="Elements of the paths in the functions tree must not"
    ):
        _flatten_functions_and_rename_parameters(functions, {})


@pytest.mark.parametrize(
    ("targets", "expected"),
    [
        (None, None),
        (
            {
                "namespace1": {"f": None, "namespace11": {"f": None}},
                "namespace2": {"f": None},
            },
            [
                "namespace1__f",
                "namespace1__namespace11__f",
                "namespace2__f",
            ],
        ),
    ],
)
def test_flatten_targets(
    targets: NestedTargetDict | None, expected: list[str] | None
) -> None:
    assert _flatten_targets_to_qual_names(targets) == expected


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
        "flat_function_name_to_check",
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
    flat_function_name_to_check: str,
    expected_argument_name: str,
) -> None:
    flat_functions = _flatten_functions_and_rename_parameters(
        functions_tree, input_structure
    )
    assert (
        expected_argument_name
        in inspect.signature(flat_functions[flat_function_name_to_check]).parameters
    )
