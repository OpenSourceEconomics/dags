"""Type definitions for DAG tree module."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import Any, Literal, TypeVar

    from dags.typing import GenericCallable

    # Type variables
    T = TypeVar("T")

    # Basic nested structure types
    NestedStrDict = Mapping[str, Any | "NestedStrDict"]

    # Flat dictionaries with qualified names or tree paths
    FlatQNDict = dict[str, Any]
    FlatTPDict = dict[tuple[str, ...], Any]

    # Function-related types
    NestedFunctionDict = Mapping[str, GenericCallable | "NestedFunctionDict"]
    FlatFunctionDict = dict[str, GenericCallable]

    # Input structure types
    NestedInputStructureDict = Mapping[str, None | "NestedInputStructureDict"]
    FlatInputStructureDict = dict[str, None]

    # Target types
    NestedTargetDict = Mapping[str, None | "NestedTargetDict"]
    FlatTargetList = list[str]

    # Input and output types
    NestedInputDict = Mapping[str, Any | "NestedInputDict"]
    NestedOutputDict = Mapping[str, Any | "NestedOutputDict"]

    # Specifies whether inputs should be in the global or local namespace
    GlobalOrLocal = Literal["global", "local"]
