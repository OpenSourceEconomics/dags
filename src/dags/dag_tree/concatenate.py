"""Functionality for concatenating functions in a DAG tree."""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Literal

from dags.dag import (
    _create_arguments_of_concatenated_function,
    concatenate_functions,
    create_dag,
)
from dags.dag_tree.parameters import (
    _create_parameter_name_mapper,
    _link_parameter_to_function_or_input,
)
from dags.dag_tree.qualified_names import (
    QUAL_NAME_DELIMITER,
    flatten_to_qual_names,
    unflatten_from_qual_names,
)
from dags.dag_tree.validation import (
    _check_for_parent_child_name_clashes,
    _fail_if_branches_have_trailing_undersores,
)
from dags.signature import rename_arguments

if TYPE_CHECKING:
    from collections.abc import Callable

    import networkx as nx

    from dags.dag_tree.typing import (
        FlatFunctionDict,
        FlatInputStructureDict,
        FlatTargetList,
        GlobalOrLocal,
        NestedFunctionDict,
        NestedInputDict,
        NestedInputStructureDict,
        NestedOutputDict,
        NestedTargetDict,
    )


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
    _fail_if_branches_have_trailing_undersores(functions)

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


def create_input_structure_tree(
    functions: NestedFunctionDict,
    targets: NestedTargetDict | None = None,
    namespace_of_inputs: GlobalOrLocal = "local",
) -> NestedInputStructureDict:
    """Create a nested input structure template based on the functions and targets.

    Args:
        functions: A nested dictionary of functions.
        targets: A nested dictionary of targets (or None).
        namespace_of_inputs: Whether to use "global" or "local" namespace for inputs.

    Returns
    -------
        A nested dictionary representing the expected input structure.
    """
    _fail_if_branches_have_trailing_undersores(functions)

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
                namespace_of_inputs,
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
