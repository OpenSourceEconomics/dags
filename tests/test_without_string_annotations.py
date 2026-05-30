"""Check behavior of annotations functions when annotations are not strings.

Do not add "from __future__ import annotations" to this file, otherwise we cannot
check what happens when annotations are not strings.

"""

import inspect

from dags.dag import FunctionExecutionInfo, concatenate_functions


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


def test_concatenate_functions_set_annotations_converts_non_string_annotations() -> (
    None
):
    """`set_annotations=True` converts non-string annotations to strings.

    Regression test for the documented behavior: non-string annotations (here
    real `int`/`float` type objects, because this module does not use
    `from __future__ import annotations`) must be converted to their string
    representation rather than raising. See ``NonStringAnnotationError``, which
    is reserved but not currently raised.
    """

    def a() -> int:
        return 1

    def b(a: int) -> float:
        return a + 1.5

    combined = concatenate_functions(
        {"a": a, "b": b},
        targets="b",
        set_annotations=True,
    )

    signature = inspect.signature(combined)
    # `a` is a function (target ancestor), so the combined function has no free
    # arguments. The propagated return annotation is the string form of `float`.
    assert list(signature.parameters) == []
    assert signature.return_annotation == "float"
    assert combined() == 2.5
