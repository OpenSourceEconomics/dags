import functools
from collections.abc import Callable
from typing import Literal

import pytest
from dags.dag_tree import _check_for_parent_child_name_clashes
from dags.dag_tree import _create_parameter_name_mapper
from dags.dag_tree import _find_parent_child_name_clashes
from dags.dag_tree import _flatten_str_dict
from dags.dag_tree import _flatten_targets
from dags.dag_tree import _get_namespace_and_simple_name
from dags.dag_tree import _get_qualified_name
from dags.dag_tree import _is_python_identifier
from dags.dag_tree import _is_qualified_name
from dags.dag_tree import _link_parameter_to_function_or_input
from dags.dag_tree import _map_parameter
from dags.dag_tree import _unflatten_str_dict
from dags.dag_tree import concatenate_functions_tree
from dags.dag_tree import create_input_structure_tree
from dags.dag_tree import FlatFunctionDict
from dags.dag_tree import FlatInputStructureDict
from dags.dag_tree import GlobalOrLocal
from dags.dag_tree import NestedFunctionDict
from dags.dag_tree import NestedInputDict
from dags.dag_tree import NestedInputStructureDict
from dags.dag_tree import NestedOutputDict
from dags.dag_tree import NestedTargetDict


# Fixtures & Other Test Inputs


def _global__f(g, namespace1__f1, input_, namespace1__input):
    """A global function with the same simple name as other functions."""

    return {
        "name": "global__f",
        "args": {
            "g": g,
            "namespace1__f1": namespace1__f1,
            "input_": input_,
            "namespace1__input": namespace1__input,
        },
    }


def _global__g():
    """A global function with a unique simple name."""

    return {"name": "global__g"}


def _namespace1__f(
    g, namespace1__f1, namespace2__f2, namespace1__input, namespace2__input2
):
    """A namespaced function with the same simple name as other functions."""

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
    """A namespaced function with a unique simple name."""

    return {"name": "namespace1__f1"}


def _namespace2__f(f2, input_):
    """A namespaced function with the same simple name as other functions. All arguments
    use simple names.
    """

    return {"name": "namespace2__f", "args": {"f2": f2, "input_": input_}}


def _namespace2__f2():
    """A namespaced function with a unique simple name."""

    return {"name": "namespace2__f2"}


def _namespace1__deep__f():
    """A deeply nested function."""

    return {"name": "namespace1_deep__f"}


@pytest.fixture
def functions() -> NestedFunctionDict:
    return {
        "f": _global__f,
        "g": _global__g,
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


# Tests


@pytest.mark.parametrize(
    ("targets", "input_", "expected"),
    [
        (
            None,
            {
                "input_": "namespace1__input",
                "namespace1": {
                    "input": "namespace1__input",
                },
                "namespace2": {
                    "input_": "namespace2__input",
                    "input2": "namespace2__input2",
                },
            },
            {
                "f": {
                    "name": "global__f",
                    "args": {
                        "g": {"name": "global__g"},
                        "namespace1__f1": {"name": "namespace1__f1"},
                        "input_": "namespace1__input",
                        "namespace1__input": "namespace1__input",
                    },
                },
                "g": {"name": "global__g"},
                "namespace1": {
                    "f": {
                        "name": "namespace1__f",
                        "args": {
                            "g": {"name": "global__g"},
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
                            "input_": "namespace2__input",
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
                "input_": "global__input",
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
                            "g": {"name": "global__g"},
                            "namespace1__f1": {"name": "namespace1__f1"},
                            "namespace2__f2": {"name": "namespace2__f2"},
                            "namespace1__input": "namespace1__input",
                            "namespace2__input2": "namespace2__input2",
                        },
                    }
                },
                "namespace2": {
                    "f": {
                        "name": "namespace2__f",
                        "args": {
                            "f2": {"name": "namespace2__f2"},
                            "input_": "global__input",
                        },
                    }
                },
            },
        ),
    ],
)
def test_concatenate_functions_tree(
    functions: NestedFunctionDict,
    targets: NestedTargetDict,
    input_: NestedInputDict,
    expected: NestedOutputDict,
):
    f = concatenate_functions_tree(functions, targets, input_, name_clashes="ignore")
    assert f(input_) == expected


@pytest.mark.parametrize(
    ("functions", "input_structure", "name_clashes"),
    [
        ({"x": lambda x: x, "nested__x": lambda x: x}, {}, "raise"),
        ({"nested__x": lambda x: x, "nested__deep__x": lambda x: x}, {}, "raise"),
        ({"x": lambda x: x}, {"nested__x": None}, "raise"),
        ({}, {"x": None, "nested__x": None}, "raise"),
    ],
)
def test_check_for_parent_child_name_clashes_error(
    functions: FlatFunctionDict,
    input_structure: FlatInputStructureDict,
    name_clashes: Literal["raise"],
):
    with pytest.raises(ValueError, match="There are name clashes:"):
        _check_for_parent_child_name_clashes(functions, input_structure, name_clashes)


@pytest.mark.parametrize(
    ("functions", "input_structure", "name_clashes"),
    [
        ({"x": lambda x: x, "nested__x": lambda x: x}, {}, "warn"),
        ({"nested__x": lambda x: x, "nested__deep__x": lambda x: x}, {}, "warn"),
        ({"x": lambda x: x}, {"nested__x": None}, "warn"),
        ({}, {"x": None, "nested__x": None}, "warn"),
    ],
)
def test_check_for_parent_child_name_clashes_warn(
    functions: FlatFunctionDict,
    input_structure: FlatInputStructureDict,
    name_clashes: Literal["warn"],
):
    with pytest.warns(UserWarning, match="There are name clashes:"):
        _check_for_parent_child_name_clashes(functions, input_structure, name_clashes)


@pytest.mark.parametrize(
    ("functions", "input_structure", "name_clashes"),
    [
        ({"x": lambda x: x, "nested__x": lambda x: x}, {}, "ignore"),
        ({"nested__x": lambda x: x, "nested__deep__x": lambda x: x}, {}, "ignore"),
        ({"x": lambda x: x}, {"nested__x": None}, "ignore"),
        ({}, {"x": None, "nested__x": None}, "ignore"),
        ({"x": lambda x: x}, {"y": None}, "raise"),
    ],
)
def test_check_for_parent_child_name_clashes_no_error(
    functions: FlatFunctionDict,
    input_structure: FlatInputStructureDict,
    name_clashes: Literal["raise", "ignore"],
):
    _check_for_parent_child_name_clashes(functions, input_structure, name_clashes)


@pytest.mark.parametrize(
    ("functions", "input_structure", "expected"),
    [
        ({"x": lambda x: x, "nested__x": lambda x: x}, {}, [("nested__x", "x")]),
        (
            {"nested__x": lambda x: x, "nested__deep__x": lambda x: x},
            {},
            [("nested__x", "nested__deep__x")],
        ),
        ({"x": lambda x: x}, {"nested__x": None}, [("nested__x", "x")]),
        ({}, {"x": None, "nested__x": None}, [("nested__x", "x")]),
        ({"x": lambda x: x}, {"y": None}, []),
    ],
)
def test_find_parent_child_name_clashes(
    functions: FlatFunctionDict,
    input_structure: FlatInputStructureDict,
    expected: list[tuple[str, str]],
):
    actual = _find_parent_child_name_clashes(functions, input_structure)

    unordered_expected = [set(pair) for pair in expected]
    unordered_actual = [set(pair) for pair in actual]

    assert unordered_actual == unordered_expected


@pytest.mark.parametrize(
    ("qualified_name", "expected"),
    [
        ("", ("", "")),
        ("a", ("", "a")),
        ("a__b", ("a", "b")),
        ("a___b", ("a", "_b")),
    ],
)
def test_get_namespace_and_simple_name(qualified_name: str, expected: tuple[str, str]):
    assert _get_namespace_and_simple_name(qualified_name) == expected


@pytest.mark.parametrize(
    ("namespace", "simple_name", "expected"),
    [
        ("", "", ""),
        ("", "a", "a"),
        ("a", "b", "a__b"),
    ],
)
def test_get_qualified_name(namespace: str, simple_name: str, expected: str):
    assert _get_qualified_name(namespace, simple_name) == expected


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
    function: Callable,
    expected: dict[str, str],
):
    flat_functions = _flatten_str_dict(functions)
    flat_input_structure = _flatten_str_dict(input_structure)

    assert (
        _create_parameter_name_mapper(
            flat_functions, flat_input_structure, namespace, function
        )
        == expected
    )


def test_map_parameter_raises():
    with pytest.raises(ValueError, match="Cannot resolve parameter"):
        _map_parameter({}, {}, "x", "x")


@pytest.mark.parametrize(
    ("targets", "level_of_inputs", "expected"),
    [
        (
            None,
            "local",
            {
                "input_": None,
                "namespace1": {
                    "input": None,
                },
                "namespace2": {
                    "input_": None,
                    "input2": None,
                },
            },
        ),
        (
            None,
            "global",
            {
                "input_": None,
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
                "input_": None,
                "namespace1": {
                    "input": None,
                },
                "namespace2": {
                    "input_": None,
                },
            },
        ),
        (
            {"f": None, "namespace2": {"f": None}},
            "global",
            {
                "input_": None,
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
):
    assert create_input_structure_tree(functions, targets, level_of_inputs) == expected


def test_flatten_str_dict(functions: NestedFunctionDict):
    assert _flatten_str_dict(functions) == {
        "f": _global__f,
        "g": _global__g,
        "namespace1__f": _namespace1__f,
        "namespace1__f1": _namespace1__f1,
        "namespace1__deep__f": _namespace1__deep__f,
        "namespace2__f": _namespace2__f,
        "namespace2__f2": _namespace2__f2,
    }


def test_unflatten_str_dict(functions: NestedFunctionDict):
    assert (
        _unflatten_str_dict(
            {
                "f": _global__f,
                "g": _global__g,
                "namespace1__f": _namespace1__f,
                "namespace1__f1": _namespace1__f1,
                "namespace1__deep__f": _namespace1__deep__f,
                "namespace2__f": _namespace2__f,
                "namespace2__f2": _namespace2__f2,
            }
        )
        == functions
    )


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
def test_flatten_targets(targets, expected):
    assert _flatten_targets(targets) == expected


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
):
    flat_functions = _flatten_str_dict(functions)
    assert (
        _link_parameter_to_function_or_input(
            flat_functions, namespace, parameter_name, level_of_inputs
        )
        == expected
    )


@pytest.mark.parametrize(
    ("s", "expected"),
    [
        ("", False),
        ("1", False),
        ("_", True),
        ("_1", True),
        ("__", True),
        ("_a", True),
        ("_A", True),
        ("a", True),
        ("a1", True),
        ("a_", True),
        ("ab", True),
        ("aB", True),
        ("A", True),
        ("A1", True),
        ("A_", True),
        ("Ab", True),
        ("AB", True),
    ],
)
def test_is_python_identifier(s: str, expected: bool):
    assert _is_python_identifier(s) == expected


@pytest.mark.parametrize(
    ("s", "expected"),
    [
        ("a", False),
        ("__", False),
        ("a__", False),
        ("__a", False),
        ("a__b", True),
    ],
)
def test_is_qualified_name(s: str, expected: bool):
    assert _is_qualified_name(s) == expected


def test_partialled_function_argument():
    def f(a, b):
        return a + b

    f_partial = functools.partial(f, b=1)
    tree = {"f": f_partial}
    input_structure = {"a": None}
    targets = {"f": None}

    concatenated_func = concatenate_functions_tree(tree, targets, input_structure)
    concatenated_func({"a": 1})
