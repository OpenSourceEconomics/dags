from collections.abc import Callable
from typing import Any, Literal, ParamSpec, TypeVar

GenericCallable = Callable[..., Any]
FunctionCollection = dict[str, GenericCallable] | list[GenericCallable]
TargetType = str | list[str] | None
CombinedFunctionReturnType = Literal["tuple", "list", "dict"]


# P captures the parameter types, R captures the return type
P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")
