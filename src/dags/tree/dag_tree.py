"""Functionality for concatenating functions in a DAG tree."""

from __future__ import annotations

import functools
import inspect
from typing import TYPE_CHECKING

from dags import dag
from dags.signature import rename_arguments
from dags.tree.tree_utils import (
    flatten_to_qual_names,
    flatten_to_tree_paths,
    qual_name_from_tree_path,
    qual_names,
    tree_path_from_qual_name,
    tree_paths,
    unflatten_from_qual_names,
    unflatten_from_tree_paths,
)
from dags.tree.validation import (
    fail_if_path_elements_have_trailing_undersores,
    fail_if_top_level_elements_repeated_in_paths,
    fail_if_top_level_elements_repeated_in_single_path,
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
    tree_path_functions = flatten_to_tree_paths(functions)
    tree_paths = set(tree_path_functions.keys())
    top_level_namespace = _get_top_level_namespace_initial(
        tree_paths=tree_paths,
        top_level_inputs=set(top_level_inputs),
    )

    # Check the paths defined in the functions tree
    fail_if_path_elements_have_trailing_undersores(tree_paths)
    fail_if_top_level_elements_repeated_in_paths(
        top_level_namespace=top_level_namespace,
        tree_paths=tree_paths,
    )

    # Now go through everything that is defined via the functions' signatures.
    tree_path_input_structure: TreePathInputStructureDict = {}
    for path, func in tree_path_functions.items():
        parameter_names = dict(inspect.signature(func).parameters).keys()

        for parameter_name in parameter_names:
            parameter_path = _get_parameter_tree_path(
                parameter_name=parameter_name,
                current_namespace=path[:-1],
                top_level_namespace=top_level_namespace,
            )

            if parameter_path not in tree_paths:
                fail_if_top_level_elements_repeated_in_single_path(
                    top_level_namespace=top_level_namespace,
                    tree_path=parameter_path,
                )
                tree_path_input_structure[parameter_path] = None

    nested_input_structure = unflatten_from_tree_paths(tree_path_input_structure)

    # If no targets are specified, all inputs are needed
    if targets is None:
        return nested_input_structure

    qual_name_functions = functions_for_dags_concatenate_functions(
        functions=functions,
        input_structure=nested_input_structure,
        top_level_namespace=top_level_namespace,
        perform_checks=False,
    )
    dag_tree = create_dag_tree(
        functions=functions,
        targets=targets,
        input_structure=nested_input_structure,
        perform_checks=False,
    )
    parameters = dag.create_arguments_of_concatenated_function(
        functions=qual_name_functions, dag=dag_tree
    )
    return unflatten_from_qual_names(dict.fromkeys(parameters))


def create_dag_tree(
    functions: NestedFunctionDict,
    targets: NestedTargetDict | None,
    input_structure: NestedInputStructureDict,
    perform_checks: bool,
) -> nx.DiGraph[str]:
    """Build a DAG from the given functions, targets, and input structure.

    Args:
        functions: A nested dictionary of functions.
        targets: A nested dictionary of targets (or None).
        input_structure: A nested dictionary describing the input structure.
        perform_checks: Check whether path elements are valid or not.

    Returns
    -------
        A networkx.DiGraph representing the DAG.
    """
    top_level_namespace = _get_top_level_namespace_final(
        functions=functions,
        input_structure=input_structure,
    )
    qual_name_functions = functions_for_dags_concatenate_functions(
        functions=functions,
        input_structure=input_structure,
        top_level_namespace=top_level_namespace,
        perform_checks=perform_checks,
    )
    qual_name_targets = qual_names(targets) if targets is not None else None

    return dag.create_dag(qual_name_functions, qual_name_targets)


def concatenate_functions_tree(
    functions: NestedFunctionDict,
    targets: NestedTargetDict | None,
    input_structure: NestedInputStructureDict,
    perform_checks: bool,
    enforce_signature: bool = True,
) -> Callable[[NestedInputDict], NestedOutputDict]:
    """Combine a nested dictionary of functions into a single callable.

    Args:
        functions: The nested dictionary of functions to concatenate.
        targets: The nested dictionary of targets (or None).
        input_structure: A nested dictionary that defines the expected input structure.
        perform_checks: Check whether path elements are valid or not.
        enforce_signature: Whether to enforce the function signature strictly.

    Returns
    -------
        A callable that takes a NestedInputDict and returns a NestedOutputDict.
    """
    top_level_namespace = _get_top_level_namespace_final(
        functions=functions,
        input_structure=input_structure,
    )
    qual_name_targets = qual_names(targets) if targets is not None else None

    qual_name_functions = functions_for_dags_concatenate_functions(
        functions=functions,
        input_structure=input_structure,
        top_level_namespace=top_level_namespace,
        perform_checks=perform_checks,
    )

    concatenated_function = dag.concatenate_functions(
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


def _get_top_level_namespace_initial(
    tree_paths: set[tuple[str, ...]],
    top_level_inputs: set[str],
) -> set[str]:
    """Get the namespace of the top level.

    Args:
        tree_paths: The set of tree paths.
        top_level_inputs: A set of input names in the top-level namespace.

    Returns
    -------
        The elements of the top-level namespace.
    """
    top_level_elements_from_functions = {path[0] for path in tree_paths}
    return top_level_elements_from_functions | top_level_inputs


def _get_top_level_namespace_final(
    functions: NestedFunctionDict, input_structure: NestedInputStructureDict
) -> set[str]:
    all_tree_paths = set(tree_paths(functions)) | set(tree_paths(input_structure))
    return {path[0] for path in all_tree_paths}


def functions_for_dags_concatenate_functions(
    functions: NestedFunctionDict,
    input_structure: NestedInputStructureDict,
    top_level_namespace: set[str],
    perform_checks: bool,
) -> QualNameFunctionDict:
    """Return a functions dictoary that `dags.concatenate` functions can work with.

    In particular, remove all tree logic by
    1. Flattening the set of functions and inputs to qualified absolute names
    2. Convert all functions so they will only take qualified absolute names as
       arguments.

    The result can be put into `dags.dag.concatenate_functions`.

    Args:
        functions: A nested dictionary of functions. input_structure: A nested
        dictionary describing the input structure. top_level_namespace: The elements of
        the top level namespace. perform_checks: Check whether path elements are valid
        or not.


    Returns
    -------
        A flat dictionary mapping qualified absolute names to functions taking
        qualified absolute names as arguments.

    """
    tree_path_functions = flatten_to_tree_paths(functions)

    if perform_checks:
        all_paths = set(tree_path_functions.keys()) | set(tree_paths(input_structure))
        fail_if_path_elements_have_trailing_undersores(all_paths)
        fail_if_top_level_elements_repeated_in_paths(
            top_level_namespace=top_level_namespace,
            tree_paths=all_paths,
        )

    qual_name_functions = {}
    for path, func in tree_path_functions.items():
        renamed = rename_arguments(
            func,
            mapper=_get_parameter_rel_to_abs_mapper(
                func=func,
                current_namespace=path[:-1],
                top_level_namespace=top_level_namespace,
            ),
        )
        qual_name_functions[qual_name_from_tree_path(path)] = renamed

    return qual_name_functions


def _get_parameter_rel_to_abs_mapper(
    func: GenericCallable,
    current_namespace: tuple[str, ...],
    top_level_namespace: set[str],
) -> dict[str, str]:
    """Create a mapping from potentially relative parameter names to absolute names.

    Args:
        func: The function for which the parameters are being mapped.
        current_namespace: The function's namespace.
        top_level_namespace: The elements of the top level namespace.

    Returns
    -------
        A dictionary mapping original parameter names to new qualified names.
    """
    return {
        old_name: _get_parameter_absolute_path(
            parameter_name=old_name,
            current_namespace=current_namespace,
            top_level_namespace=top_level_namespace,
        )
        for old_name in dag.get_free_arguments(func)
    }


def _get_parameter_absolute_path(
    parameter_name: str,
    current_namespace: tuple[str, ...],
    top_level_namespace: set[str],
) -> str:
    """Map a parameter name to its qualified version. Raise an error if not present.

    Args:
        parameter_name:
            The name of the parameter, potentially qualified.
        current_namespace:
            The namespace that contains the function that contains the parameter.
        top_level_namespace:
            The elements of the top level namespace.

    Returns
    -------
        The qualified parameter name.

    """
    return qual_name_from_tree_path(
        _get_parameter_tree_path(
            parameter_name=parameter_name,
            current_namespace=current_namespace,
            top_level_namespace=top_level_namespace,
        )
    )


def _get_parameter_tree_path(
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
    # Just a tuple since it could be a relative or an absolute path.
    parameter_tuple = tree_path_from_qual_name(parameter_name)

    if parameter_tuple[0] in top_level_namespace:
        parameter_tree_path = parameter_tuple
    else:
        parameter_tree_path = current_namespace + parameter_tuple

    return parameter_tree_path
