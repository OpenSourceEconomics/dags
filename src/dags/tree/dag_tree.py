"""Functionality for concatenating functions in a DAG tree."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dags.dag import (
    concatenate_functions,
    create_arguments_of_concatenated_function,
    create_dag,
    create_execution_info,
    get_annotations_from_execution_info,
    get_free_arguments,
    harmonize_and_check_functions_and_targets,
)
from dags.signature import rename_arguments
from dags.tree.tree_utils import (
    flatten_to_qnames,
    flatten_to_tree_paths,
    qname_from_tree_path,
    qnames,
    tree_path_from_qname,
    tree_paths,
    unflatten_from_qnames,
)
from dags.tree.validation import (
    fail_if_paths_are_invalid,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    import networkx as nx

    from dags.tree.typing import (
        NestedFunctionDict,
        NestedInputDict,
        NestedInputStructureDict,
        NestedOutputDict,
        NestedTargetDict,
        QNameFunctionDict,
    )


def create_tree_with_input_types(
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
    top_level_namespace = _get_top_level_namespace_initial(
        functions=functions,
        top_level_inputs=set(top_level_inputs),
    )
    functions_for_flat_dags = functions_without_tree_logic(
        functions=functions,
        top_level_namespace=top_level_namespace,
    )
    fail_if_paths_are_invalid(
        functions=functions,
        abs_qnames_functions=functions_for_flat_dags,
        targets=targets,
        top_level_namespace=top_level_namespace,
    )

    targets_qnames = qnames(targets) if targets is not None else None

    _functions, _targets = harmonize_and_check_functions_and_targets(
        functions=functions_for_flat_dags,
        targets=targets_qnames,
    )

    dag = create_dag(
        functions=_functions,
        targets=_targets,
    )
    arglist = create_arguments_of_concatenated_function(functions=_functions, dag=dag)
    execution_info = create_execution_info(_functions, dag)
    args = get_annotations_from_execution_info(
        execution_info,
        arglist=arglist,
        targets=_targets,
    )[0]
    return unflatten_from_qnames(args)


def create_dag_tree(
    functions: NestedFunctionDict,
    inputs: NestedInputDict,
    targets: NestedTargetDict | None,
) -> nx.DiGraph[str]:
    """Build a DAG from the given functions, targets, and input structure.

    Args:
        functions: A nested dictionary of functions.
        inputs: A nested dictionary with the inputs or their structure.
        targets: A nested dictionary of targets (or None).

    Returns
    -------
        A networkx.DiGraph representing the DAG.
    """
    top_level_namespace = _get_top_level_namespace_final(
        functions=functions,
        inputs=inputs,
    )
    functions_for_flat_dags = functions_without_tree_logic(
        functions=functions,
        top_level_namespace=top_level_namespace,
    )
    targets_for_flat_dags = qnames(targets) if targets is not None else None

    return create_dag(functions_for_flat_dags, targets_for_flat_dags)


def concatenate_functions_tree(
    functions: NestedFunctionDict,
    inputs: NestedInputDict,
    targets: NestedTargetDict | None,
    enforce_signature: bool = True,
) -> Callable[[NestedInputDict], NestedOutputDict]:
    """Combine a nested dictionary of functions into a single callable.

    Args:
        functions: The nested dictionary of functions to concatenate.
        inputs: A nested dictionary that defines the (structure of) inputs.
        targets: The nested dictionary of targets (or None).
        enforce_signature: Whether to enforce the function signature strictly.

    Returns
    -------
        A callable that takes a NestedInputDict and returns a NestedOutputDict.
    """
    top_level_namespace = _get_top_level_namespace_final(
        functions=functions,
        inputs=inputs,
    )
    functions_for_flat_dags = functions_without_tree_logic(
        functions=functions,
        top_level_namespace=top_level_namespace,
    )
    targets_for_flat_dags = qnames(targets) if targets is not None else None

    concatenated_function = concatenate_functions(
        functions=functions_for_flat_dags,
        targets=targets_for_flat_dags,
        return_type="dict",
        enforce_signature=enforce_signature,
    )

    def wrapper(inputs: NestedInputDict) -> NestedOutputDict:
        qname_inputs = flatten_to_qnames(inputs)
        qname_outputs = concatenated_function(**qname_inputs)
        return unflatten_from_qnames(qname_outputs)

    return wrapper


def functions_without_tree_logic(
    functions: NestedFunctionDict,
    top_level_namespace: set[str],
) -> QNameFunctionDict:
    """Return a functions dictionary that `dags.concatenate_functions` can work with.

    In particular, remove all tree logic by

    1. Flattening the set of functions and inputs to qualified absolute names.
    2. Convert all functions so they take only qualified absolute names as arguments.

    The result can be put into `dags.concatenate_functions`.

    Args:
        functions:
            A nested dictionary of functions.
        top_level_namespace:
            The elements of the top-level namespace.


    Returns
    -------
    A flat dictionary mapping qualified absolute names to functions taking qualified
    absolute names as arguments.

    """
    tree_path_functions = flatten_to_tree_paths(functions)

    qname_functions = {}
    for path, func in tree_path_functions.items():
        renamed = one_function_without_tree_logic(
            function=func,
            tree_path=path,
            top_level_namespace=top_level_namespace,
        )
        qname_functions[qname_from_tree_path(path)] = renamed

    return qname_functions


def one_function_without_tree_logic(
    function: Callable[..., Any],
    tree_path: tuple[str, ...],
    top_level_namespace: set[str],
) -> Callable[..., Any]:
    """Convert a single function to work without tree logic.

    Args:
        func: The function to convert.
        path: The tree path of the function.
        top_level_namespace: The elements of the top level namespace.

    Returns
    -------
        A function that takes qualified absolute names as arguments.
    """
    return rename_arguments(
        func=function,
        mapper=_map_parameters_rel_to_abs(
            func=function,
            current_namespace=tree_path[:-1],
            top_level_namespace=top_level_namespace,
        ),
    )


def _get_top_level_namespace_initial(
    functions: NestedFunctionDict,
    top_level_inputs: set[str],
) -> set[str]:
    """Get the elements of the top-level namespace.

    Args:
        functions: The nested dictionary of functions.
        top_level_inputs: A set of input names in the top-level namespace.

    Returns
    -------
        The elements of the top-level namespace.
    """
    paths = set(tree_paths(functions))
    top_level_elements_from_functions = {path[0] for path in paths}
    return top_level_elements_from_functions | top_level_inputs


def _get_top_level_namespace_final(
    functions: NestedFunctionDict, inputs: NestedInputStructureDict
) -> set[str]:
    all_tree_paths = set(tree_paths(functions)) | set(tree_paths(inputs))
    return {path[0] for path in all_tree_paths}


def _map_parameters_rel_to_abs(
    func: Callable[..., Any],
    current_namespace: tuple[str, ...],
    top_level_namespace: set[str],
) -> dict[str, str]:
    """Map (potentially) relative parameter names to qualified absolute names.

    Args:
        func: The function for which the parameters are being mapped.
        current_namespace: The function's namespace.
        top_level_namespace: The elements of the top level namespace.

    Returns
    -------
        A dictionary mapping original parameter names to qualified absolute names.
    """
    return {
        old_name: _get_parameter_absolute_path(
            parameter_name=old_name,
            current_namespace=current_namespace,
            top_level_namespace=top_level_namespace,
        )
        for old_name in get_free_arguments(func)
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
    return qname_from_tree_path(
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
    parameter_tuple = tree_path_from_qname(parameter_name)

    if parameter_tuple[0] in top_level_namespace:
        parameter_tree_path = parameter_tuple
    else:
        parameter_tree_path = current_namespace + parameter_tuple

    return parameter_tree_path
