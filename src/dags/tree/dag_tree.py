"""Functionality for concatenating functions in a DAG tree."""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Literal

from dags.dag import (
    _create_arguments_of_concatenated_function,
    _get_free_arguments,
    concatenate_functions,
    create_dag,
)
from dags.signature import rename_arguments
from dags.tree.tree_utils import (
    QUAL_NAME_DELIMITER,
    _is_qualified_name,
    flatten_to_qual_names,
    unflatten_from_qual_names,
)
from dags.tree.validation import (
    _check_for_parent_child_name_clashes,
    fail_if_path_elements_have_trailing_undersores,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    import networkx as nx

    from dags.tree.typing import (
        FlatFunctionDict,
        FlatInputStructureDict,
        FlatTargetList,
        GenericCallable,
        NestedFunctionDict,
        NestedInputDict,
        NestedInputStructureDict,
        NestedOutputDict,
        NestedTargetDict,
    )


def create_input_structure_tree(
    functions: NestedFunctionDict,
    targets: NestedTargetDict | None = None,
) -> NestedInputStructureDict:
    """Create a nested input structure template based on the functions and targets.

    Args:
        functions: A nested dictionary of functions.
        targets: A nested dictionary of targets (or None).

    Returns
    -------
        A nested dictionary representing the expected input structure.
    """
    fail_if_path_elements_have_trailing_undersores(functions)

    flat_functions = flatten_to_qual_names(functions)
    flat_input_structure: FlatInputStructureDict = {}

    import inspect

    for path, func in flat_functions.items():
        namespace = QUAL_NAME_DELIMITER.join(
            path.split(QUAL_NAME_DELIMITER)[:-1],
        )
        parameter_names = dict(inspect.signature(func).parameters).keys()

        for parameter_name in parameter_names:
            parameter_path = _link_parameter_to_function_or_input(
                flat_functions,
                namespace,
                parameter_name,
            )

            if parameter_path not in flat_functions:
                flat_input_structure[parameter_path] = None

    nested_input_structure = unflatten_from_qual_names(flat_input_structure)

    # If no targets are specified, all inputs are needed
    if targets is None:
        return nested_input_structure

    # Compute transitive hull of inputs needed for given targets
    flat_renamed_functions = _flatten_functions_and_rename_parameters(
        functions,
        nested_input_structure,
        name_clashes="ignore",
    )
    flat_targets = _flatten_targets_to_qual_names(targets)
    dag = create_dag(flat_renamed_functions, flat_targets)
    parameters = _create_arguments_of_concatenated_function(flat_renamed_functions, dag)

    return unflatten_from_qual_names(dict.fromkeys(parameters))


def create_dag_tree(
    functions: NestedFunctionDict,
    targets: NestedTargetDict | None,
    input_structure: NestedInputStructureDict,
    name_clashes: Literal["raise", "warn", "ignore"] = "raise",
) -> nx.DiGraph[str]:
    """Build a DAG from the given functions, targets, and input structure.

    Args:
        functions: A nested dictionary of functions.
        targets: A nested dictionary of targets (or None).
        input_structure: A nested dictionary describing the input structure.
        name_clashes: How to handle name clashes.

    Returns
    -------
        A networkx.DiGraph representing the DAG.
    """
    flat_functions = _flatten_functions_and_rename_parameters(
        functions,
        input_structure,
        name_clashes,
    )
    flat_targets = _flatten_targets_to_qual_names(targets)

    return create_dag(flat_functions, flat_targets)


def concatenate_functions_tree(
    functions: NestedFunctionDict,
    targets: NestedTargetDict | None,
    input_structure: NestedInputStructureDict,
    name_clashes: Literal["raise", "warn", "ignore"] = "raise",
    enforce_signature: bool = True,
) -> Callable[[NestedInputDict], NestedOutputDict]:
    """Combine a nested dictionary of functions into a single callable.

    Args:
        functions: The nested dictionary of functions to concatenate.
        targets: The nested dictionary of targets (or None).
        input_structure: A nested dictionary that defines the expected input structure.
        name_clashes: How to handle name clashes ("raise", "warn", or "ignore").
        enforce_signature: Whether to enforce the function signature strictly.

    Returns
    -------
        A callable that takes a NestedInputDict and returns a NestedOutputDict.
    """
    flat_functions: FlatFunctionDict = _flatten_functions_and_rename_parameters(
        functions,
        input_structure,
        name_clashes,
    )
    flat_targets: FlatTargetList | None = _flatten_targets_to_qual_names(targets)

    concatenated_function = concatenate_functions(
        flat_functions,
        flat_targets,
        return_type="dict",
        enforce_signature=enforce_signature,
    )

    @functools.wraps(concatenated_function)
    def wrapper(inputs: NestedInputDict) -> NestedOutputDict:
        flat_inputs = flatten_to_qual_names(inputs)
        flat_outputs = concatenated_function(**flat_inputs)
        return unflatten_from_qual_names(flat_outputs)

    return wrapper


def _flatten_functions_and_rename_parameters(
    functions: NestedFunctionDict,
    input_structure: NestedInputStructureDict,
    name_clashes: Literal["raise", "warn", "ignore"] = "raise",
) -> FlatFunctionDict:
    """Flatten the nested function dictionary and rename parameters to avoid collisions.

    Args:
        functions: A nested dictionary of functions.
        input_structure: A nested dictionary describing the input structure.
        name_clashes: How to handle name clashes.

    Returns
    -------
        A flat dictionary mapping function names to functions.
    """
    fail_if_path_elements_have_trailing_undersores(functions)

    flat_functions: FlatFunctionDict = flatten_to_qual_names(functions)
    flat_input_structure: FlatInputStructureDict = flatten_to_qual_names(
        input_structure
    )

    _check_for_parent_child_name_clashes(
        flat_functions=flat_functions,
        flat_input_structure=flat_input_structure,
        name_clashes_resolution=name_clashes,
    )

    for name, func in flat_functions.items():
        namespace: str = QUAL_NAME_DELIMITER.join(name.split(QUAL_NAME_DELIMITER)[:-1])
        renamed = rename_arguments(
            func,
            mapper=_create_parameter_name_mapper(
                flat_functions=flat_functions,
                flat_input_structure=flat_input_structure,
                namespace=namespace,
                func=func,
            ),
        )
        flat_functions[name] = renamed

    return flat_functions


def _flatten_targets_to_qual_names(
    targets: NestedTargetDict | None,
) -> FlatTargetList | None:
    """Flatten targets to a list of qualified names.

    Args:
        targets: Nested dictionary of targets

    Returns
    -------
        List of qualified names or None
    """
    if targets is None:
        return None

    return list(flatten_to_qual_names(targets).keys())


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
) -> str:
    """Return the path to the function/input that the parameter points to.

    If the parameter name has double underscores (e.g. "namespace1__f"), we know it
    represents a qualified name and the path simply consists of the segments of the
    qualified name (e.g. "namespace1, "f").

    Otherwise, we cannot be sure whether the parameter points to a function/input of
    the current namespace or a function/input of the top level. In this case, we
        (1) look for a function with that name in the current namespace,
        (2) look for a function with that name in the top level, and
        (3) assume the parameter points to an input in the current namespace.

    Args:
        flat_functions:
            The flat dictionary of functions.
        namespace:
            The namespace that contains the function that contains the parameter.
        parameter_name:
            The name of the parameter.

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

    return namespaced_parameter
