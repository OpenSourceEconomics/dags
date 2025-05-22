from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal, ParamSpec, TypeVar

from typing_extensions import TypeVarTuple

GenericCallable = Callable[..., Any]
FunctionCollection = dict[str, GenericCallable] | list[GenericCallable]
TargetType = str | list[str] | None
CombinedFunctionReturnType = Literal["tuple", "list", "dict"]


# ParamSpec representing the full signature (positional and keyword parameters) of a
# callable
P = ParamSpec("P")
# TypeVar representing the return type of a callable
R = TypeVar("R")
# Generic type variable for use in type constructors
T = TypeVar("T")
# Variadic TypeVar for tuples of arbitrary length with heterogeneous element types
MixedTupleType = TypeVarTuple("MixedTupleType")
