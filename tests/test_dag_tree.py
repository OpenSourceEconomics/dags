import pytest
from dags.dag_tree import _compute_path_for_parameter, concatenate_functions_tree
from dags.dag_tree import _flatten_functions
from dags.dag_tree import _is_python_identifier
from dags.dag_tree import _is_qualified_name
from dags.dag_tree import create_input_structure_tree
from dags.dag_tree import NestedFunctionDict
from dags.dag_tree import NestedInputStructureDict
from dags.dag_tree import TopOrNamespace


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
    """
    A namespaced function with the same simple name as other functions. All arguments use
    simple names.
    """

    return {"name": "namespace2__f", "args": {"f2": f2, "input_": input_}}


def _namespace2__f2():
    """A namespaced function with a unique simple name."""

    return {"name": "namespace2__f2"}


@pytest.fixture
def functions() -> NestedFunctionDict:
    return {
        "f": _global__f,
        "g": _global__g,
        "namespace1": {
            "f": _namespace1__f,
            "f1": _namespace1__f1,
        },
        "namespace2": {
            "f": _namespace2__f,
            "f2": _namespace2__f2,
        },
    }


# Tests


def test_concatenate_functions_tree():
    pass


@pytest.mark.parametrize(
    "level_of_inputs, expected",
    [
        (
            "namespace",
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
            "top",
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
    ],
)
def test_create_input_structure_tree(
    functions: NestedFunctionDict,
    level_of_inputs: TopOrNamespace,
    expected: NestedInputStructureDict,
):
    assert create_input_structure_tree(functions, level_of_inputs) == expected


def test_flatten_functions(functions):
    assert _flatten_functions(functions) == {
        ("f",): _global__f,
        ("g",): _global__g,
        ("namespace1", "f"): _namespace1__f,
        ("namespace1", "f1"): _namespace1__f1,
        ("namespace2", "f"): _namespace2__f,
        ("namespace2", "f2"): _namespace2__f2,
    }


@pytest.mark.parametrize(
    "level_of_inputs, namespace_path, parameter_name, expected",
    [
        ("namespace", ("namespace1",), "namespace1__f1", ("namespace1", "f1")),
        ("namespace", ("namespace1",), "f1", ("namespace1", "f1")),
        ("namespace", ("namespace1",), "g", ("g",)),
        ("namespace", ("namespace1",), "input", ("namespace1", "input")),
        ("top", ("namespace1",), "input", ("input",)),
    ],
)
def test_compute_path_for_parameter(
    functions: NestedFunctionDict,
    level_of_inputs: TopOrNamespace,
    namespace_path: tuple[str],
    parameter_name: str,
    expected: tuple[str],
):
    flat_functions = _flatten_functions(functions)
    assert (
        _compute_path_for_parameter(
            flat_functions, namespace_path, parameter_name, level_of_inputs
        )
        == expected
    )


@pytest.mark.parametrize(
    "s, expected",
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
def test_is_python_identifier(s, expected):
    assert _is_python_identifier(s) == expected


@pytest.mark.parametrize(
    "s, expected",
    [
        ("a", False),
        ("__", False),
        ("a__", False),
        ("__a", False),
        ("a__b", True),
    ],
)
def test_is_qualified_name(s, expected):
    assert _is_qualified_name(s) == expected
