import pytest

from dags.dag_tree import _is_qualified_name, _is_python_identifier


def test_concatenate_functions_tree():
    pass


def test_create_input_structure_tree():
    pass


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
    ]
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
    ]
)
def test_is_qualified_name(s, expected):
    assert _is_qualified_name(s) == expected
