"""Check behavior of annotations functions when annotations are not strings.

Do not add "from __future__ import annotations" to this file, otherwise we cannot
check what happens when annotations are not strings.

"""

from dags.dag import FunctionExecutionInfo


def test_function_execution_info() -> None:
    def f(a: int, b: float) -> float:
        return a + b

    FunctionExecutionInfo(
        name="f",
        func=f,
        verify_annotations=False,
    )


def test_function_execution_info_verify_annotations_converts_to_strings() -> None:
    """Test that verify_annotations=True converts non-string annotations to strings."""

    def f(a: int, b: float) -> float:
        return a + b

    info = FunctionExecutionInfo(
        name="f",
        func=f,
        verify_annotations=True,
    )
    # Annotations should now be strings
    assert info.annotations == {"a": "int", "b": "float", "return": "float"}
