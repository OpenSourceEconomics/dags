import pytest

from dags.dag import FunctionExecutionInfo
from dags.exceptions import NonStringAnnotationError


def test_function_execution_info() -> None:
    def f(a: int, b: float) -> float:
        return a + b

    FunctionExecutionInfo(
        name="f",
        func=f,
        verify_annotations=False,
    )


def test_function_execution_info_verify_annotations() -> None:
    def f(a: int, b: float) -> float:
        return a + b

    with pytest.raises(NonStringAnnotationError):
        FunctionExecutionInfo(
            name="f",
            func=f,
            verify_annotations=True,
        )
