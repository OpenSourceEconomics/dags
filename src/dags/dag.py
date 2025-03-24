from __future__ import annotations

import functools
import inspect
import textwrap
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import networkx as nx

from dags.output import aggregated_output, dict_output, list_output, single_output
from dags.signature import with_signature

if TYPE_CHECKING:
    from collections.abc import Callable

    from dags.typing import (
        CombinedFunctionReturnType,
        FunctionCollection,
        GenericCallable,
        T,
        TargetType,
    )


@dataclass
class FunctionExecutionInfo:
    """Information about a function that is needed to execute it.

    Attributes
    ----------
        func: The function to execute.
        arguments: The names of the arguments of the function.

    """

    func: GenericCallable
    arguments: list[str]


def concatenate_functions(
    functions: FunctionCollection,
    targets: TargetType = None,
    return_type: CombinedFunctionReturnType = "tuple",
    aggregator: Callable[[T, T], T] | None = None,
    enforce_signature: bool = True,
) -> GenericCallable:
    """Combine functions to one function that generates targets.

    Functions can depend on the output of other functions as inputs, as long as the
    dependencies can be described by a directed acyclic graph (DAG).

    Functions that are not required to produce the targets will simply be ignored.

    The arguments of the combined function are all arguments of relevant functions that
    are not themselves function names, in alphabetical order.

    Args:
        functions (dict or list): Dict or list of functions. If a list, the function
            name is inferred from the __name__ attribute of the entries. If a dict, the
            name of the function is set to the dictionary key.
        targets (str or list or None): Name of the function that produces the target or
            list of such function names. If the value is `None`, all variables are
            returned.
        return_type (str): One of "tuple", "list", "dict". This is ignored if the
            targets are a single string or if an aggregator is provided.
        aggregator (callable or None): Binary reduction function that is used to
            aggregate the targets into a single target.
        enforce_signature (bool): If True, the signature of the concatenated function
            is enforced. Otherwise it is only provided for introspection purposes.
            Enforcing the signature has a small runtime overhead.

    Returns
    -------
        function: A function that produces targets when called with suitable arguments.

    """
    # Create the DAG.
    dag = create_dag(functions, targets)

    # Build combined function.
    return _create_combined_function_from_dag(
        dag,
        functions,
        targets,
        return_type,
        aggregator,
        enforce_signature,
    )


def create_dag(
    functions: FunctionCollection,
    targets: TargetType,
) -> nx.DiGraph[str]:
    """Build a directed acyclic graph (DAG) from functions.

    Functions can depend on the output of other functions as inputs, as long as the
    dependencies can be described by a directed acyclic graph (DAG).

    Functions that are not required to produce the targets will simply be ignored.

    Args:
        functions (dict or list): Dict or list of functions. If a list, the function
            name is inferred from the __name__ attribute of the entries. If a dict, the
            name of the function is set to the dictionary key.
        targets (str or list or None): Name of the function that produces the target or
            list of such function names. If the value is `None`, all variables are
            returned.

    Returns
    -------
        dag: the DAG (as networkx.DiGraph object)

    """
    # Harmonize and check arguments.
    _functions, _targets = _harmonize_and_check_functions_and_targets(
        functions,
        targets,
    )

    # Create the DAG
    _raw_dag = _create_complete_dag(_functions)
    dag = _limit_dag_to_targets_and_their_ancestors(_raw_dag, _targets)

    # Check if there are cycles in the DAG
    _fail_if_dag_contains_cycle(dag)

    return dag


def _create_combined_function_from_dag(
    dag: nx.DiGraph[str],
    functions: FunctionCollection,
    targets: TargetType,
    return_type: CombinedFunctionReturnType = "tuple",
    aggregator: Callable[[T, T], T] | None = None,
    enforce_signature: bool = True,
) -> GenericCallable:
    """Create combined function which allows executing a DAG in one function call.

    The arguments of the combined function are all arguments of relevant functions that
    are not themselves function names, in alphabetical order.

    Args:
        dag (networkx.DiGraph): a DAG of functions
        functions (dict or list): Dict or list of functions. If a list, the function
            name is inferred from the __name__ attribute of the entries. If a dict, the
            name of the function is set to the dictionary key.
        targets (str or list or None): Name of the function that produces the target or
            list of such function names. If the value is `None`, all variables are
            returned.
        return_type (str): One of "tuple", "list", "dict". This is ignored if the
            targets are a single string or if an aggregator is provided.
        aggregator (callable or None): Binary reduction function that is used to
            aggregate the targets into a single target.
        enforce_signature (bool): If True, the signature of the concatenated function
            is enforced. Otherwise it is only provided for introspection purposes.
            Enforcing the signature has a small runtime overhead.

    Returns
    -------
        function: A function that produces targets when called with suitable arguments.

    """
    # Harmonize and check arguments.
    _functions, _targets = _harmonize_and_check_functions_and_targets(
        functions,
        targets,
    )

    _arglist = create_arguments_of_concatenated_function(_functions, dag)
    _exec_info = _create_execution_info(_functions, dag)
    _concatenated = _create_concatenated_function(
        _exec_info,
        _arglist,
        _targets,
        enforce_signature,
    )

    # Return function in specified format.
    if isinstance(targets, str) or (aggregator is not None and len(_targets) == 1):
        out = cast("GenericCallable", single_output(_concatenated))
    elif aggregator is not None:
        out = cast(
            "GenericCallable", aggregated_output(_concatenated, aggregator=aggregator)
        )
    elif return_type == "list":
        out = cast("GenericCallable", list_output(_concatenated))
    elif return_type == "tuple":
        out = _concatenated
    elif return_type == "dict":
        out = cast("GenericCallable", dict_output(_concatenated, keys=_targets))
    else:
        msg = (
            f"Invalid return type {return_type}. Must be 'list', 'tuple', or 'dict'. "
            f"You provided {return_type}."
        )
        raise ValueError(msg)

    return out


def get_ancestors(
    functions: FunctionCollection,
    targets: TargetType,
    include_targets: bool = False,
) -> set[str]:
    """Build a DAG and extract all ancestors of targets.

    Args:
        functions (dict or list): Dict or list of functions. If a list, the function
            name is inferred from the __name__ attribute of the entries. If a dict,
            with node names as keys or just the values as a tuple for multiple outputs.
        targets (str): Name of the function that produces the target function.
        include_targets (bool): Whether to include the target as its own ancestor.

    Returns
    -------
        set: The ancestors

    """
    # Harmonize and check arguments.
    _functions, _targets = _harmonize_and_check_functions_and_targets(
        functions,
        targets,
    )

    # Create the DAG.
    dag = create_dag(functions, targets)

    ancestors: set[str] = set()
    for target in _targets:
        ancestors = ancestors.union(nx.ancestors(dag, target))
        if include_targets:
            ancestors.add(target)
    return ancestors


def _harmonize_and_check_functions_and_targets(
    functions: FunctionCollection,
    targets: TargetType,
) -> tuple[dict[str, GenericCallable], list[str]]:
    """Harmonize the type of specified functions and targets and do some checks.

    Args:
        functions (dict or list): Dict or list of functions. If a list, the function
            name is inferred from the __name__ attribute of the entries. If a dict, the
            name of the function is set to the dictionary key.
        targets (str or list): Name of the function that produces the target or list of
            such function names.

    Returns
    -------
        functions_harmonized: harmonized functions
        targets_harmonized: harmonized targets

    """
    functions_harmonized = _harmonize_functions(functions)
    targets_harmonized = _harmonize_targets(targets, list(functions_harmonized))
    _fail_if_targets_have_wrong_types(targets_harmonized)
    _fail_if_functions_are_missing(functions_harmonized, targets_harmonized)

    return functions_harmonized, targets_harmonized


def _harmonize_functions(
    functions: FunctionCollection,
) -> dict[str, GenericCallable]:
    if not isinstance(functions, dict):
        functions_dict = {func.__name__: func for func in functions}
    else:
        functions_dict = functions

    return functions_dict


def _harmonize_targets(
    targets: TargetType,
    function_names: list[str],
) -> list[str]:
    if targets is None:
        targets = function_names
    elif isinstance(targets, str):
        targets = [targets]
    return targets


def _fail_if_targets_have_wrong_types(
    targets: list[str],
) -> None:
    not_strings = [target for target in targets if not isinstance(target, str)]
    if not_strings:
        msg = f"Targets must be strings. The following targets are not: {not_strings}"
        raise ValueError(msg)


def _fail_if_functions_are_missing(
    functions: dict[str, GenericCallable],
    targets: list[str],
) -> None:
    targets_not_in_functions = set(targets) - set(functions)
    if targets_not_in_functions:
        formatted = _format_list_linewise(list(targets_not_in_functions))
        msg = f"The following targets have no corresponding function:\n{formatted}"
        raise ValueError(msg)


def _fail_if_dag_contains_cycle(dag: nx.DiGraph[str]) -> None:
    """Check for cycles in DAG."""
    cycles = list(nx.simple_cycles(dag))

    if len(cycles) > 0:
        formatted = _format_list_linewise(cycles)
        msg = f"The DAG contains one or more cycles:\n{formatted}"
        raise ValueError(msg)


def _create_complete_dag(
    functions: dict[str, GenericCallable],
) -> nx.DiGraph[str]:
    """Create the complete DAG.

    This DAG is constructed from all functions and not pruned by specified root nodes or
    targets.

    Args:
        functions (dict): Dictionary containing functions to build the DAG.

    Returns
    -------
        networkx.DiGraph: The complete DAG

    """
    functions_arguments_dict = {
        name: get_free_arguments(function) for name, function in functions.items()
    }
    return nx.DiGraph(functions_arguments_dict).reverse()  # type: ignore[arg-type]


def get_free_arguments(
    func: GenericCallable,
) -> list[str]:
    arguments = list(inspect.signature(func).parameters)
    if isinstance(func, functools.partial):
        # arguments that are partialled by position are not part of the signature
        # anyways, so they do not need special handling.
        non_free = set(func.keywords)
        arguments = [arg for arg in arguments if arg not in non_free]

    return arguments


def _limit_dag_to_targets_and_their_ancestors(
    dag: nx.DiGraph[str],
    targets: list[str],
) -> nx.DiGraph[str]:
    """Limit DAG to targets and their ancestors.

    Args:
        dag (networkx.DiGraph): The complete DAG.
        targets (str): Variable of interest.

    Returns
    -------
        networkx.DiGraph: The pruned DAG.

    """
    used_nodes = set(targets)
    for target in targets:
        used_nodes = used_nodes | set(nx.ancestors(dag, target))

    all_nodes = set(dag.nodes)

    unused_nodes = all_nodes - used_nodes

    dag.remove_nodes_from(unused_nodes)

    return dag


def create_arguments_of_concatenated_function(
    functions: dict[str, GenericCallable],
    dag: nx.DiGraph[str],
) -> list[str]:
    """Create the signature of the concatenated function.

    Args:
        functions (dict): Dictionary containing functions to build the DAG.
        dag (networkx.DiGraph): The complete DAG.

    Returns
    -------
        list: The arguments of the concatenated function.

    """
    function_names = set(functions)
    all_nodes = set(dag.nodes)
    return sorted(all_nodes - function_names)


def _create_execution_info(
    functions: dict[str, GenericCallable],
    dag: nx.DiGraph[str],
) -> dict[str, FunctionExecutionInfo]:
    """Create a dictionary with all information needed to execute relevant functions.

    Args:
        functions (dict): Dictionary containing functions to build the DAG.
        dag (networkx.DiGraph): The complete DAG.

    Returns
    -------
        dict: Dictionary with functions and their arguments for each node in the DAG.
            The functions are already in topological_sort order.

    """
    out = {}
    for node in nx.topological_sort(dag):
        if node in functions:
            arguments = get_free_arguments(functions[node])
            out[node] = FunctionExecutionInfo(func=functions[node], arguments=arguments)
    return out


def _create_concatenated_function(
    execution_info: dict[str, FunctionExecutionInfo],
    arglist: list[str],
    targets: list[str],
    enforce_signature: bool,
) -> Callable[..., tuple[Any, ...]]:
    """Create a concatenated function object with correct signature.

    Args:
        execution_info: Dataclass with functions and their arguments for each
            node in the DAG. The functions are already in topological_sort order.
        arglist: The list of arguments of the concatenated function.
        targets: List that is used to determine what is returned and the
            order of the outputs.
        enforce_signature: If True, the signature of the concatenated function
            is enforced. Otherwise it is only provided for introspection purposes.
            Enforcing the signature has a small runtime overhead.

    Returns
    -------
        The concatenated function

    """

    @with_signature(args=arglist, enforce=enforce_signature)
    def concatenated(*args: Any, **kwargs: Any) -> tuple[Any, ...]:  # noqa: ANN401
        results = {**dict(zip(arglist, args, strict=False)), **kwargs}
        for name, info in execution_info.items():
            func_kwargs = {arg: results[arg] for arg in info.arguments}
            result = info.func(**func_kwargs)
            results[name] = result

        return tuple(results[target] for target in targets)

    return concatenated


def _format_list_linewise(
    list_: list[object],
) -> str:
    formatted_list = '",\n    "'.join([str(c) for c in list_])
    return textwrap.dedent(
        """
        [
            "{formatted_list}",
        ]
        """,
    ).format(formatted_list=formatted_list)
