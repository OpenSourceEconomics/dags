from collections.abc import Callable, Mapping
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
NestedFunctionDict = Mapping[str, Union[GenericCallable, "NestedFunctionDict"]]
FlatFunctionDict = dict[str, GenericCallable]

NestedTargetDict = Mapping[str, Union[None, "NestedTargetDict"]]
FlatTargetList = list[str]

NestedInputStructureDict = Mapping[str, Union[None, "NestedInputStructureDict"]]
FlatInputStructureDict = dict[str, None]

NestedInputDict = Mapping[str, Union[Any, "NestedInputDict"]]
NestedOutputDict = Mapping[str, Union[Any, "NestedOutputDict"]]

NestedStrDict = Mapping[str, Union[Any, "NestedStrDict"]]
FlatStrDict = dict[str, Any]

GlobalOrLocal = Literal["global", "local"]
