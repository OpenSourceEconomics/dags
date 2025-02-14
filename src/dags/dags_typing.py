from collections.abc import Callable
from typing import Any, Literal, ParamSpec, TypeVar, Union

GenericCallable = Callable[..., Any]
FunctionCollection = dict[str, GenericCallable] | list[GenericCallable]
TargetType = str | list[str] | None
CombinedFunctionReturnType = Literal["tuple", "list", "dict"]


# P captures the parameter types, R captures the return type
P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")


# DAG-Tree typing
NestedFunctionDict = dict[str, Union[GenericCallable, "NestedFunctionDict"]]
FlatFunctionDict = dict[str, GenericCallable]

NestedTargetDict = dict[str, Union[None, "NestedTargetDict"]]
FlatTargetList = list[str]

NestedInputStructureDict = dict[str, Union[None, "NestedInputStructureDict"]]
FlatInputStructureDict = dict[str, None]

NestedInputDict = dict[str, Union[Any, "NestedInputDict"]]
NestedOutputDict = dict[str, Union[Any, "NestedOutputDict"]]

NestedStrDict = dict[str, Union[Any, "NestedStrDict"]]
FlatStrDict = dict[str, Any]

GlobalOrLocal = Literal["global", "local"]
