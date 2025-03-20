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
    flatten_to_tree_paths,
    tree_path_from_qual_name,
    unflatten_from_qual_names,
    unflatten_from_tree_paths,
)
from dags.tree.validation import (
    _check_for_parent_child_name_clashes,
    fail_if_path_elements_have_trailing_undersores,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    import networkx as nx

    from dags.tree.typing import (
        GenericCallable,
        NestedFunctionDict,
        NestedInputDict,
        NestedInputStructureDict,
        NestedOutputDict,
        NestedTargetDict,
        QualNameFunctionDict,
        QualNameInputStructureDict,
        QualNameTargetList,
        TreePathFunctionDict,
        TreePathInputStructureDict,
    )


def create_input_structure_tree(
    functions: NestedFunctionDict,
    targets: NestedTargetDict | None = None,
    top_level_inputs: set[str] | list[str] | tuple[str, ...] = (),
) -> NestedInputStructureDict:
    """Create a nested input structure template based on the functions and targets.

    Args:
        functions: A nested dictionary of functions.
        targets: A nested dictionary of targets (or None).
        top_level_inputs: Names of inputs in the top-level namespace.

    Returns
    -------
        A nested dictionary representing the expected input structure.
    """
    fail_if_path_elements_have_trailing_undersores(functions)

    tree_path_functions = flatten_to_tree_paths(functions)
    top_level_namespace = _get_top_level_namespace(
        tree_path_functions=tree_path_functions,
        top_level_inputs=set(top_level_inputs),
    )

    import inspect

    tree_path_input_structure: TreePathInputStructureDict = {}
    for path, func in tree_path_functions.items():
        parameter_names = dict(inspect.signature(func).parameters).keys()

        for parameter_name in parameter_names:
            parameter_path = _link_parameter_to_function_or_input(
                parameter_name=parameter_name,
                current_namespace=path[:-1],
                top_level_namespace=top_level_namespace,
            )

            if parameter_path not in tree_path_functions:
                tree_path_input_structure[parameter_path] = None

    nested_input_structure = unflatten_from_tree_paths(tree_path_input_structure)

    # If no targets are specified, all inputs are needed
    if targets is None:
        return nested_input_structure

    # Compute transitive hull of inputs needed for given targets
    qual_name_renamed_functions = _flatten_functions_and_rename_parameters(
        functions,
        nested_input_structure,
        name_clashes="ignore",
    )
    qual_name_targets = _flatten_targets_to_qual_names(targets)
    dag = create_dag(qual_name_renamed_functions, qual_name_targets)
    parameters = _create_arguments_of_concatenated_function(
        qual_name_renamed_functions, dag
    )

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
    qual_name_functions = _flatten_functions_and_rename_parameters(
        functions,
        input_structure,
        name_clashes,
    )
    qual_name_targets = _flatten_targets_to_qual_names(targets)

    return create_dag(qual_name_functions, qual_name_targets)


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
    qual_name_functions: QualNameFunctionDict = (
        _flatten_functions_and_rename_parameters(
            functions,
            input_structure,
            name_clashes,
        )
    )
    qual_name_targets: QualNameTargetList | None = _flatten_targets_to_qual_names(
        targets
    )

    concatenated_function = concatenate_functions(
        qual_name_functions,
        qual_name_targets,
        return_type="dict",
        enforce_signature=enforce_signature,
    )

    @functools.wraps(concatenated_function)
    def wrapper(inputs: NestedInputDict) -> NestedOutputDict:
        qual_name_inputs = flatten_to_qual_names(inputs)
        qual_name_outputs = concatenated_function(**qual_name_inputs)
        return unflatten_from_qual_names(qual_name_outputs)

    return wrapper


def _get_top_level_namespace(
    tree_path_functions: TreePathFunctionDict,
    top_level_inputs: set[str],
) -> set[str]:
    """Get the namespace of the top level.

    Args:
        tree_path_functions: A dictionary of mapping tree paths to functions.
        top_level_inputs: A set of input names in the top-level namespace.

    Returns
    -------
        The elements of the top-level namespace.
    """
    top_level_elements_from_functions = {path[0] for path in tree_path_functions}
    return top_level_elements_from_functions | top_level_inputs


def _flatten_functions_and_rename_parameters(
    functions: NestedFunctionDict,
    input_structure: NestedInputStructureDict,
    name_clashes: Literal["raise", "warn", "ignore"] = "raise",
) -> QualNameFunctionDict:
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

    qual_name_functions: QualNameFunctionDict = flatten_to_qual_names(functions)
    qual_name_input_structure: QualNameInputStructureDict = flatten_to_qual_names(
        input_structure
    )

    _check_for_parent_child_name_clashes(
        qual_name_functions=qual_name_functions,
        qual_name_input_structure=qual_name_input_structure,
        name_clashes_resolution=name_clashes,
    )

    for name, func in qual_name_functions.items():
        namespace: str = QUAL_NAME_DELIMITER.join(name.split(QUAL_NAME_DELIMITER)[:-1])
        renamed = rename_arguments(
            func,
            mapper=_create_parameter_name_mapper(
                qual_name_functions=qual_name_functions,
                qual_name_input_structure=qual_name_input_structure,
                namespace=namespace,
                func=func,
            ),
        )
        qual_name_functions[name] = renamed

    return qual_name_functions


def _flatten_targets_to_qual_names(
    targets: NestedTargetDict | None,
) -> QualNameTargetList | None:
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
    qual_name_functions: QualNameFunctionDict,
    qual_name_input_structure: QualNameInputStructureDict,
    namespace: str,
    func: GenericCallable,
) -> dict[str, str]:
    """Create a mapping from parameter names to qualified names for a given function.

    Args:
        qual_name_functions: A flat dictionary of functions.
        qual_name_input_structure: A flat dictionary of the input structure.
        namespace: The namespace to prepend.
        func: The function whose parameters are being mapped.

    Returns
    -------
        A dictionary mapping original parameter names to new qualified names.
    """
    return {
        old_name: _map_parameter(
            qual_name_functions,
            qual_name_input_structure,
            namespace,
            old_name,
        )
        for old_name in _get_free_arguments(func)
    }


def _map_parameter(
    qual_name_functions: QualNameFunctionDict,
    qual_name_input_structure: QualNameInputStructureDict,
    namespace: str,
    parameter_name: str,
) -> str:
    """Map a single parameter name to its qualified version.

    Args:
        qual_name_functions: A flat dictionary of functions.
        qual_name_input_structure: A flat dictionary of the input structure.
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
    if namespaced_parameter in qual_name_functions:
        return namespaced_parameter

    # (1.2) Look for input in the current namespace
    if namespaced_parameter in qual_name_input_structure:
        return namespaced_parameter

    # (2.1) Look for function in the top level
    if parameter_name in qual_name_functions:
        return parameter_name

    # (2.2) Look for input in the top level
    if parameter_name in qual_name_input_structure:
        return parameter_name

    # (3) Raise error
    msg = f"Cannot resolve parameter {parameter_name}"
    raise ValueError(msg)


def _link_parameter_to_function_or_input(
    parameter_name: str,
    current_namespace: tuple[str, ...],
    top_level_namespace: set[str],
) -> tuple[str, ...]:
    """Return the path to the function/input that the parameter points to.

    If the first element of the parameter's tree path can be found in the top-level
    namespace, it will be interpreted as an absolute path.

    Otherwise, the path is relative to the current namespace.

    Args:
        parameter_name:
            The name of the parameter, potentially qualified.
        current_namespace:
            The namespace that contains the function that contains the parameter.
        top_level_namespace:
            The elements of the top level namespace.

    Returns
    -------
        The path to the function/input that the parameter points to.
    """
    parameter_tuple = tree_path_from_qual_name(parameter_name)

    if parameter_tuple[0] in top_level_namespace:
        parameter_tree_path = parameter_tuple
    else:
        parameter_tree_path = current_namespace + parameter_tuple

    return parameter_tree_path
