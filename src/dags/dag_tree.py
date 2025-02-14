from __future__ import annotations

import functools
import inspect
import re
import warnings
from itertools import combinations, groupby
from operator import itemgetter
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from collections.abc import Callable

    import networkx as nx

    from dags.dags_typing import (
        FlatFunctionDict,
        FlatInputStructureDict,
        FlatStrDict,
        FlatTargetList,
        GenericCallable,
        GlobalOrLocal,
        NestedFunctionDict,
        NestedInputDict,
        NestedInputStructureDict,
        NestedOutputDict,
        NestedStrDict,
        NestedTargetDict,
    )

from flatten_dict import flatten, unflatten
from flatten_dict.reducers import make_reducer
from flatten_dict.splitters import make_splitter

from dags.dag import (
    _create_arguments_of_concatenated_function,
    _get_free_arguments,
    concatenate_functions,
    create_dag,
)
from dags.signature import rename_arguments

# Constants for qualified names.
_python_identifier: str = r"[a-zA-Z_][a-zA-Z0-9_]*"
_qualified_name_delimiter: str = "__"
_qualified_name: str = (
    f"{_python_identifier}(?:{_qualified_name_delimiter}{_python_identifier})+"
)

# Reducers and splitters to flatten/unflatten dicts with qualified names as keys.
_qualified_name_reducer = make_reducer(delimiter=_qualified_name_delimiter)
_qualified_name_splitter = make_splitter(delimiter=_qualified_name_delimiter)


def concatenate_functions_tree(
    functions: NestedFunctionDict,
    targets: NestedTargetDict | None,
    input_structure: NestedInputStructureDict,
    name_clashes: Literal["raise", "warn", "ignore"] = "raise",
    enforce_signature: bool = True,
) -> Callable[[NestedInputDict], NestedOutputDict]:
    """
    Combine a nested dictionary of functions into a single callable.

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
    flat_targets: FlatTargetList | None = _flatten_targets(targets)

    concatenated_function = concatenate_functions(
        flat_functions,
        flat_targets,
        return_type="dict",
        enforce_signature=enforce_signature,
    )

    @functools.wraps(concatenated_function)
    def wrapper(inputs: NestedInputDict) -> NestedOutputDict:
        flat_inputs: FlatStrDict = _flatten_str_dict(inputs)
        flat_outputs: FlatStrDict = concatenated_function(**flat_inputs)
        return _unflatten_str_dict(flat_outputs)

    return wrapper


def _flatten_functions_and_rename_parameters(
    functions: NestedFunctionDict,
    input_structure: NestedInputStructureDict,
    name_clashes: Literal["raise", "warn", "ignore"] = "raise",
) -> FlatFunctionDict:
    """
    Flatten the nested function dictionary and rename parameters to avoid collisions.

    Args:
        functions: A nested dictionary of functions.
        input_structure: A nested dictionary describing the input structure.
        name_clashes: How to handle name clashes.

    Returns
    -------
        A flat dictionary mapping function names to functions.
    """
    _fail_if_branches_have_trailing_undersores(functions)

    flat_functions: FlatFunctionDict = _flatten_str_dict(functions)
    flat_input_structure: FlatInputStructureDict = _flatten_str_dict(input_structure)

    _check_for_parent_child_name_clashes(
        flat_functions,
        flat_input_structure,
        name_clashes,
    )

    for name, func in flat_functions.items():
        namespace: str = _qualified_name_delimiter.join(
            name.split(_qualified_name_delimiter)[:-1]
        )
        renamed: GenericCallable = rename_arguments(
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


def _check_for_parent_child_name_clashes(
    flat_functions: FlatFunctionDict,
    flat_input_structure: FlatInputStructureDict,
    name_clashes_resolution: Literal["raise", "warn", "ignore"],
) -> None:
    """
    Raise an error if name clashes exist between parent and child functions/inputs.

    Args:
        flat_functions: A flat dictionary of functions.
        flat_input_structure: A flat dictionary of the input structure.
        name_clashes_resolution: Strategy for resolving name clashes.
    """
    if name_clashes_resolution == "ignore":
        return

    name_clashes: list[tuple[str, str]] = _find_parent_child_name_clashes(
        flat_functions, flat_input_structure
    )

    if len(name_clashes) > 0:
        message: str = f"There are name clashes: {name_clashes}."
        if name_clashes_resolution == "raise":
            raise ValueError(message)
        if name_clashes_resolution == "warn":
            warnings.warn(message, stacklevel=2)


def _find_parent_child_name_clashes(
    flat_functions: FlatFunctionDict,
    flat_input_structure: FlatInputStructureDict,
) -> list[tuple[str, str]]:
    """
    Find and return a list of tuples representing parent-child name clashes.

    Args:
        flat_functions: A flat dictionary of functions.
        flat_input_structure: A flat dictionary of the input structure.

    Returns
    -------
        A list of tuples, each containing two qualified names that clash.
    """
    _qualified_names: set[str] = set(flat_functions.keys()) | set(
        flat_input_structure.keys()
    )
    namespace_and_simple_names: list[tuple[str, str]] = [
        _get_namespace_and_simple_name(qname) for qname in _qualified_names
    ]
    # Group by simple name (only functions/inputs with the same simple name can clash)
    namespace_and_simple_names.sort(key=itemgetter(1))
    grouped_by_simple_name = groupby(namespace_and_simple_names, key=itemgetter(1))
    result: list[tuple[str, str]] = []
    for _, group in grouped_by_simple_name:
        group_list = list(group)
        for pair in combinations(group_list, 2):
            namespace_1, simple_name_1 = pair[0]
            namespace_2, simple_name_2 = pair[1]
            if namespace_1.startswith(namespace_2) or namespace_2.startswith(
                namespace_1
            ):
                result.append(
                    (
                        _get_qualified_name(namespace_1, simple_name_1),
                        _get_qualified_name(namespace_2, simple_name_2),
                    )
                )
    return result


def _get_namespace_and_simple_name(qualified_name: str) -> tuple[str, str]:
    """
    Split a qualified name into its namespace and simple name.

    Args:
        qualified_name: A string representing the fully qualified name.

    Returns
    -------
        A tuple containing the namespace and the simple name.
    """
    segments: list[str] = qualified_name.split(_qualified_name_delimiter)
    if len(segments) == 1:
        return "", segments[0]
    namespace: str = _qualified_name_delimiter.join(segments[:-1])
    simple_name: str = segments[-1]
    return namespace, simple_name


def _get_qualified_name(namespace: str, simple_name: str) -> str:
    """
    Combine a namespace and a simple name into a fully qualified name.

    Args:
        namespace: The namespace component.
        simple_name: The simple name component.

    Returns
    -------
        The fully qualified name.
    """
    if namespace:
        return f"{namespace}{_qualified_name_delimiter}{simple_name}"
    return simple_name


def _create_parameter_name_mapper(
    flat_functions: FlatFunctionDict,
    flat_input_structure: FlatInputStructureDict,
    namespace: str,
    func: GenericCallable,
) -> dict[str, str]:
    """
    Create a mapping from parameter names to qualified names for a given function.

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
    """
    Map a single parameter name to its qualified version.

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


def create_input_structure_tree(
    functions: NestedFunctionDict,
    targets: NestedTargetDict | None = None,
    namespace_of_inputs: GlobalOrLocal = "local",
) -> NestedInputStructureDict:
    """
    Create a nested input structure template based on the functions and targets.

    Args:
        functions: A nested dictionary of functions.
        targets: A nested dictionary of targets (or None).
        namespace_of_inputs: Whether to use "global" or "local" namespace for inputs.

    Returns
    -------
        A nested dictionary representing the expected input structure.
    """
    _fail_if_branches_have_trailing_undersores(functions)

    flat_functions = _flatten_str_dict(functions)
    flat_input_structure: FlatInputStructureDict = {}

    for path, func in flat_functions.items():
        namespace = _qualified_name_delimiter.join(
            path.split(_qualified_name_delimiter)[:-1],
        )
        parameter_names = dict(inspect.signature(func).parameters).keys()

        for parameter_name in parameter_names:
            parameter_path = _link_parameter_to_function_or_input(
                flat_functions,
                namespace,
                parameter_name,
                namespace_of_inputs,
            )

            if parameter_path not in flat_functions:
                flat_input_structure[parameter_path] = None

    nested_input_structure = _unflatten_str_dict(flat_input_structure)

    # If no targets are specified, all inputs are needed
    if targets is None:
        return nested_input_structure

    # Compute transitive hull of inputs needed for given targets
    flat_renamed_functions = _flatten_functions_and_rename_parameters(
        functions,
        nested_input_structure,
        name_clashes="ignore",
    )
    flat_targets = _flatten_targets(targets)
    dag = create_dag(flat_renamed_functions, flat_targets)
    parameters = _create_arguments_of_concatenated_function(flat_renamed_functions, dag)

    return _unflatten_str_dict({parameter: None for parameter in parameters})


def create_dag_tree(
    functions: NestedFunctionDict,
    targets: NestedTargetDict | None,
    input_structure: NestedInputStructureDict,
    name_clashes: Literal["raise", "warn", "ignore"] = "raise",
) -> nx.DiGraph:
    """
    Build a DAG from the given functions, targets, and input structure.

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
    flat_targets = _flatten_targets(targets)

    return create_dag(flat_functions, flat_targets)


def _flatten_str_dict(str_dict: NestedStrDict) -> FlatStrDict:
    return flatten(str_dict, reducer=_qualified_name_reducer)


def _unflatten_str_dict(str_dict: FlatStrDict) -> NestedStrDict:
    return unflatten(str_dict, splitter=_qualified_name_splitter)


def _flatten_targets(targets: NestedTargetDict | None) -> FlatTargetList | None:
    if targets is None:
        return None

    return list(_flatten_str_dict(targets).keys())


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


def _is_python_identifier(s: str) -> bool:
    return bool(re.fullmatch(_python_identifier, s))


def _is_qualified_name(s: str) -> bool:
    return bool(re.fullmatch(_qualified_name, s))


def _fail_if_branches_have_trailing_undersores(functions: FlatStrDict) -> None:
    """Raise a ValueError if any branch of the functions tree ends with an underscore.

    Args:
        functions:
            The functions tree.

    Raises
    ------
        ValueError: If any branch of the functions tree ends with an underscore.
    """
    flattened_functions_tree = flatten(functions)
    for path in flattened_functions_tree:
        if len(path) > 1 and any(name.endswith("_") for name in path[:-1]):
            msg = "Branches of the functions tree cannot end with an underscore."
            raise ValueError(msg)
