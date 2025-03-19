from __future__ import annotations

import functools
import inspect
from typing import TYPE_CHECKING, Literal

import pytest

from dags.dag_tree import (
    concatenate_functions_tree,
    create_input_structure_tree,
    flatten_to_qual_names,
    flatten_to_tree_paths,
    qual_name_from_tree_path,
    qual_names,
    tree_path_from_qual_name,
    tree_paths,
    unflatten_from_qual_names,
    unflatten_from_tree_paths,
)
from dags.dag_tree.concatenate import (
    _flatten_functions_and_rename_parameters,
    _flatten_targets_to_qual_names,
)
from dags.dag_tree.parameters import (
    _create_parameter_name_mapper,
    _link_parameter_to_function_or_input,
    _map_parameter,
)
from dags.dag_tree.qualified_names import (
    _get_namespace_and_simple_name,
    _get_qualified_name,
    _is_python_identifier,
    _is_qualified_name,
)
from dags.dag_tree.validation import (
    _check_for_parent_child_name_clashes,
    _find_parent_child_name_clashes,
)

if TYPE_CHECKING:
    from dags.dag_tree.typing import (
        FlatFunctionDict,
        FlatInputStructureDict,
        GlobalOrLocal,
        NestedFunctionDict,
        NestedInputDict,
        NestedInputStructureDict,
        NestedOutputDict,
        NestedTargetDict,
    )
    from dags.typing import GenericCallable


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


# Tests


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
) -> None:
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
) -> None:
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
) -> None:
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
) -> None:
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
def test_get_namespace_and_simple_name(
    qualified_name: str, expected: tuple[str, str]
) -> None:
    assert _get_namespace_and_simple_name(qualified_name) == expected


@pytest.mark.parametrize(
    ("namespace", "simple_name", "expected"),
    [
        ("", "", ""),
        ("", "a", "a"),
        ("a", "b", "a__b"),
    ],
)
def test_get_qualified_name(namespace: str, simple_name: str, expected: str) -> None:
    assert _get_qualified_name(namespace, simple_name) == expected


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
    flat_functions = flatten_to_qual_names(functions)
    input_structure = flatten_to_qual_names(input_structure)

    assert (
        _create_parameter_name_mapper(
            flat_functions,
            input_structure,
            namespace,
            function,
        )
        == expected
    )


def test_map_parameter_raises() -> None:
    with pytest.raises(ValueError, match="Cannot resolve parameter"):
        _map_parameter({}, {}, "x", "x")


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


def test_flatten_str_dict(functions: NestedFunctionDict) -> None:
    assert flatten_to_qual_names(functions) == {
        "f": f,
        "g": g,
        "namespace1__f": _namespace1__f,
        "namespace1__f1": _namespace1__f1,
        "namespace1__deep__f": _namespace1__deep__f,
        "namespace2__f": _namespace2__f,
        "namespace2__f2": _namespace2__f2,
    }


def test_unflatten_str_dict(functions: NestedFunctionDict) -> None:
    assert (
        unflatten_from_qual_names(
            {
                "f": f,
                "g": g,
                "namespace1__f": _namespace1__f,
                "namespace1__f1": _namespace1__f1,
                "namespace1__deep__f": _namespace1__deep__f,
                "namespace2__f": _namespace2__f,
                "namespace2__f2": _namespace2__f2,
            },
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
def test_flatten_targets(targets: NestedTargetDict, expected: list[str]) -> None:
    assert _flatten_targets_to_qual_names(targets) == expected


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
        ("ÄB", True),
        ("ä", True),
        ("äb", True),
        ("Ä", True),
        ("ß", True),
    ],
)
def test_is_python_identifier(s: str, expected: bool) -> None:
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
def test_is_qualified_name(s: str, expected: bool) -> None:
    assert _is_qualified_name(s) == expected


def test_partialled_function_argument() -> None:
    def f(a, b):
        return a + b

    f_partial = functools.partial(f, b=1)
    tree = {"f": f_partial}
    input_structure = {"a": None}
    targets = {"f": None}

    concatenated_func = concatenate_functions_tree(
        functions=tree, targets=targets, input_structure=input_structure
    )
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


def _bb() -> None:
    return None


def _ee() -> None:
    return None


def _ff() -> None:
    return None


@pytest.fixture
def functions_tree() -> NestedFunctionDict:
    return {
        "a": {"b": _bb},
        "c": {"d": {"e": _ee}},
        "f": _ff,
    }


def test_flatten_to_qual_names(functions_tree: NestedFunctionDict) -> None:
    assert flatten_to_qual_names(functions_tree) == {
        "a__b": _bb,
        "c__d__e": _ee,
        "f": _ff,
    }


def test_round_trip_via_qual_names(functions_tree: NestedFunctionDict) -> None:
    assert (
        unflatten_from_qual_names(flatten_to_qual_names(functions_tree))
        == functions_tree
    )


def test_qual_names(functions_tree: NestedFunctionDict) -> None:
    assert qual_names(functions_tree) == [
        "a__b",
        "c__d__e",
        "f",
    ]


def test_flatten_to_tree_paths(functions_tree: NestedFunctionDict) -> None:
    assert flatten_to_tree_paths(functions_tree) == {
        ("a", "b"): _bb,
        ("c", "d", "e"): _ee,
        ("f",): _ff,
    }


def test_round_trip_via_tree_paths(functions_tree: NestedFunctionDict) -> None:
    assert (
        unflatten_from_tree_paths(flatten_to_tree_paths(functions_tree))
        == functions_tree
    )


def test_tree_paths(functions_tree: NestedFunctionDict) -> None:
    assert tree_paths(functions_tree) == [
        ("a", "b"),
        ("c", "d", "e"),
        ("f",),
    ]


def test_qual_name_from_tree_path() -> None:
    assert qual_name_from_tree_path(("a", "b")) == "a__b"


def test_tree_path_from_qual_name() -> None:
    assert tree_path_from_qual_name("a__b") == ("a", "b")
