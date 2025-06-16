"""Type definitions for DAG tree module."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from dags.typing import GenericCallable

# Type variables
T = TypeVar("T")

# Basic nested structure types
NestedStructureDict = Mapping[str, "Any | NestedStructureDict"]

# Input and output types (same as NestedStructureDict, but make it more specific)
NestedInputDict = Mapping[str, "Any | NestedInputDict"]
NestedOutputDict = Mapping[str, "Any | NestedOutputDict"]

# Flat dictionaries with qualified names or tree paths
FlatQNameDict = dict[str, Any]
FlatTreePathDict = dict[tuple[str, ...], Any]

# Function-related types
NestedFunctionDict = Mapping[str, "GenericCallable | NestedFunctionDict"]
QNameFunctionDict = dict[str, GenericCallable]
TreePathFunctionDict = dict[tuple[str, ...], GenericCallable]

# Input structure types
NestedInputStructureDict = Mapping[str, "str | None | NestedInputStructureDict"]
QNameInputStructureDict = dict[str, "str | None"]
TreePathInputStructureDict = dict[tuple[str, ...], "str | None"]

# Target types
NestedTargetDict = Mapping[str, "str | None | NestedTargetDict"]
QNameTargetList = list[str]
