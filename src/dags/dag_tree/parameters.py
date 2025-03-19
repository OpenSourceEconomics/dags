"""Parameter handling for DAG trees."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dags.dag import _get_free_arguments
from dags.dag_tree.qualified_names import (
    _is_qualified_name,
)

if TYPE_CHECKING:
    from dags.dag_tree.typing import (
        FlatFunctionDict,
        FlatInputStructureDict,
        GlobalOrLocal,
    )
    from dags.typing import GenericCallable


def _create_parameter_name_mapper(
    flat_functions: FlatFunctionDict,
    flat_input_structure: FlatInputStructureDict,
    namespace: str,
    func: GenericCallable,
) -> dict[str, str]:
    """Create a mapping from parameter names to qualified names for a given function.

    Args:
        flat_functions: A flat dictionary of functions.
        flat_input_structure: A flat dictionary of the input structure.
        namespace: The namespace to prepend.
        func: The function whose parameters are being mapped.

    Returns
    -------
        A dictionary mapping original parameter names to new qualified names.
    """
    return {
        old_name: _map_parameter(
            flat_functions,
            flat_input_structure,
            namespace,
            old_name,
        )
        for old_name in _get_free_arguments(func)
    }


def _map_parameter(
    flat_functions: FlatFunctionDict,
    flat_input_structure: FlatInputStructureDict,
    namespace: str,
    parameter_name: str,
) -> str:
    """Map a single parameter name to its qualified version.

    Args:
        flat_functions: A flat dictionary of functions.
        flat_input_structure: A flat dictionary of the input structure.
        namespace: The namespace to apply.
        parameter_name: The original parameter name.

    Returns
    -------
        The qualified parameter name.
    """
    # Parameter name is definitely a qualified name
    if _is_qualified_name(parameter_name):
        return parameter_name

    # (1.1) Look for function in the current namespace
    namespaced_parameter = (
        f"{namespace}__{parameter_name}" if namespace else parameter_name
    )
    if namespaced_parameter in flat_functions:
        return namespaced_parameter

    # (1.2) Look for input in the current namespace
    if namespaced_parameter in flat_input_structure:
        return namespaced_parameter

    # (2.1) Look for function in the top level
    if parameter_name in flat_functions:
        return parameter_name

    # (2.2) Look for input in the top level
    if parameter_name in flat_input_structure:
        return parameter_name

    # (3) Raise error
    msg = f"Cannot resolve parameter {parameter_name}"
    raise ValueError(msg)


def _link_parameter_to_function_or_input(
    flat_functions: FlatFunctionDict,
    namespace: str,
    parameter_name: str,
    namespace_of_inputs: GlobalOrLocal = "local",
) -> str:
    """Return the path to the function/input that the parameter points to.

    If the parameter name has double underscores (e.g. "namespace1__f"), we know it
    represents a qualified name and the path simply consists of the segments of the
    qualified name (e.g. "namespace1, "f").

    Otherwise, we cannot be sure whether the parameter points to a function/input of
    the current namespace or a function/input of the top level. In this case, we
        (1) look for a function with that name in the current namespace,
        (2) look for a function with that name in the top level, and
        (3) assume the parameter points to an input.
    In the third case, `namespace_of_inputs` determines whether the parameter points
    to an input of the current namespace ("local") or an input of the top level
    ("global").

    Args:
        flat_functions:
            The flat dictionary of functions.
        namespace:
            The namespace that contains the function that contains the parameter.
        parameter_name:
            The name of the parameter.
        namespace_of_inputs:
            The level of inputs to assume if the parameter name does not represent a
            function.

    Returns
    -------
        The path to the function/input that the parameter points to.
    """
    # Parameter name is definitely a qualified name
    if _is_qualified_name(parameter_name):
        return parameter_name

    # (1) Look for function in the current namespace
    namespaced_parameter = (
        f"{namespace}__{parameter_name}" if namespace else parameter_name
    )
    if namespaced_parameter in flat_functions:
        return namespaced_parameter

    # (2) Look for function in the top level
    if parameter_name in flat_functions:
        return parameter_name

    # (3) Assume parameter points to an unknown input
    if namespace_of_inputs == "global":
        return parameter_name
    return namespaced_parameter
