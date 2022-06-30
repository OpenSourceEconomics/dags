import functools
import inspect
import textwrap

import networkx as nx
from dags.output import aggregated_output
from dags.output import dict_output
from dags.output import list_output
from dags.output import single_output
from dags.signature import with_signature


def concatenate_functions(
    functions,
    targets=None,
    return_type="tuple",
    aggregator=None,
    enforce_signature=True,
):
    """Combine functions to one function that generates targets.

    Functions can depend on the output of other functions as inputs, as long as the
    dependencies can be described by a directed acyclic graph (DAG).

    Functions that are not required to produce the targets will simply be ignored.

    The arguments of the combined function are all arguments of relevant functions
    that are not themselves function names, in alphabetical order.

    Args:
        functions (dict or list): Dict or list of functions. If a list, the function
            name is inferred from the __name__ attribute of the entries. If a dict,
            the name of the function is set to the dictionary key.
        targets (str | None): Name of the function that produces the target or list of
            such function names. If the value is `None`, all variables are returned.
        return_type (str): One of "tuple", "list", "dict". This is ignored if the
            targets are a single string or if an aggregator is provided.
        aggregator (callable or None): Binary reduction function that is used to
            aggregate the targets into a single target.
        enforce_signature (bool): If True, the signature of the concatenated function
            is enforced. Otherwise it is only provided for introspection purposes.
            Enforcing the signature has a small runtime overhead.

    Returns:
        function: A function that produces targets when called with suitable arguments.

    """
    _functions = _harmonize_functions(functions)
    _targets = _harmonize_targets(targets, list(_functions))
    _fail_if_targets_have_wrong_types(_targets)
    _fail_if_functions_are_missing(_functions, _targets)

    _raw_dag = _create_complete_dag(_functions)
    _dag = _limit_dag_to_targets_and_their_ancestors(_raw_dag, _targets)
    _arglist = _create_arguments_of_concatenated_function(_functions, _dag)
    _exec_info = _create_execution_info(_functions, _dag)
    _concatenated = _create_concatenated_function(
        _exec_info, _arglist, _targets, enforce_signature
    )

    if isinstance(targets, str) or (aggregator is not None and len(_targets) == 1):
        out = single_output(_concatenated)
    elif aggregator is not None:
        out = aggregated_output(_concatenated, aggregator=aggregator)
    elif return_type == "list":
        out = list_output(_concatenated)
    elif return_type == "tuple":
        out = _concatenated
    elif return_type == "dict":
        out = dict_output(_concatenated, keys=_targets)
    else:
        raise ValueError(
            f"Invalid return type {return_type}. Must be 'list', 'tuple',  or 'dict'. "
            f"You provided {return_type}."
        )

    return out


def get_ancestors(functions, targets, include_targets=False):
    """Build a DAG and extract all ancestors of targets.

    Args:
        functions (dict or list): Dict or list of functions. If a list, the function
            name is inferred from the __name__ attribute of the entries. If a dict,
            with node names as keys or just the values as a tuple for multiple outputs.
        targets (str): Name of the function that produces the target function.
        include_targets (bool): Whether to include the target as its own ancestor.

    Returns:
        set: The ancestors

    """
    _functions = _harmonize_functions(functions)
    _targets = _harmonize_targets(targets, list(_functions))
    _fail_if_targets_have_wrong_types(_targets)
    _fail_if_functions_are_missing(_functions, _targets)

    raw_dag = _create_complete_dag(_functions)
    dag = _limit_dag_to_targets_and_their_ancestors(raw_dag, _targets)

    ancestors = set()
    for target in _targets:
        ancestors = ancestors.union(nx.ancestors(dag, target))
        if include_targets:
            ancestors.add(target)
    return ancestors


def _harmonize_functions(functions):
    if isinstance(functions, (list, tuple)):
        functions = {func.__name__: func for func in functions}
    return functions


def _harmonize_targets(targets, function_names):
    if targets is None:
        targets = function_names
    elif isinstance(targets, str):
        targets = [targets]
    return targets


def _fail_if_targets_have_wrong_types(targets):
    not_strings = [target for target in targets if not isinstance(target, str)]
    if not_strings:
        raise ValueError(
            f"Targets must be strings. The following targets are not: {not_strings}"
        )


def _fail_if_functions_are_missing(functions, targets):
    # to-do: add typo suggestions via fuzzywuzzy, see estimagic
    targets_not_in_functions = set(targets) - set(functions)
    if targets_not_in_functions:
        formatted = _format_list_linewise(targets_not_in_functions)
        raise ValueError(
            f"The following targets have no corresponding function:\n{formatted}"
        )

    return functions, targets


def _create_complete_dag(functions):
    """Create the complete DAG.

    This DAG is constructed from all functions and not pruned by specified root nodes or
    targets.

    Args:
        functions (dict): Dictionary containing functions to build the DAG.

    Returns:
        networkx.DiGraph: The complete DAG

    """
    functions_arguments_dict = {
        name: _get_free_arguments(function) for name, function in functions.items()
    }
    dag = nx.DiGraph(functions_arguments_dict).reverse()

    return dag


def _get_free_arguments(func):
    arguments = list(inspect.signature(func).parameters)
    if isinstance(func, functools.partial):
        # arguments that are partialled by position are not part of the signature
        # anyways, so they do not need special handling.
        non_free = set(func.keywords)
        arguments = [arg for arg in arguments if arg not in non_free]

    return arguments


def _limit_dag_to_targets_and_their_ancestors(dag, targets):
    """Limit DAG to targets and their ancestors.

    Args:
        dag (networkx.DiGraph): The complete DAG.
        targets (str): Variable of interest.

    Returns:
        networkx.DiGraph: The pruned DAG.

    """
    used_nodes = set(targets)
    for target in targets:
        used_nodes = used_nodes | set(nx.ancestors(dag, target))

    all_nodes = set(dag.nodes)

    unused_nodes = all_nodes - used_nodes

    dag.remove_nodes_from(unused_nodes)

    return dag


def _create_arguments_of_concatenated_function(functions, dag):
    """Create the signature of the concatenated function.

    Args:
        functions (dict): Dictionary containing functions to build the DAG.
        dag (networkx.DiGraph): The complete DAG.

    Returns:
        inspect.Signature: The signature of the concatenated function.

    """
    function_names = set(functions)
    all_nodes = set(dag.nodes)
    arguments = sorted(all_nodes - function_names)
    return arguments


def _create_execution_info(functions, dag):
    """Create a dictionary with all information needed to execute relevant functions.

    Args:
        functions (dict): Dictionary containing functions to build the DAG.
        dag (networkx.DiGraph): The complete DAG.

    Returns:
        dict: Dictionary with functions and their arguments for each node in the dag.
            The functions are already in topological_sort order.

    """
    out = {}
    for node in nx.topological_sort(dag):
        if node in functions:
            arguments = _get_free_arguments(functions[node])
            info = {}
            info["func"] = functions[node]
            info["arguments"] = arguments
            out[node] = info
    return out


def _create_concatenated_function(
    execution_info,
    arglist,
    targets,
    enforce_signature,
):
    """Create a concatenated function object with correct signature.

    Args:
        execution_info (dict): Dictionary with functions and their arguments for each
            node in the dag. The functions are already in topological_sort order.
        arglist (list): The list of arguments of the concatenated function.
        targets (list): List that is used to determine what is returned and the
            order of the outputs.
        enforce_signature (bool):If True, the signature of the concatenated function
            is enforced. Otherwise it is only provided for introspection purposes.
            Enforcing the signature has a small runtime overhead.

    Returns:
        callable: The concatenated function

    """

    @with_signature(args=arglist, enforce=enforce_signature)
    def concatenated(*args, **kwargs):
        results = {**dict(zip(arglist, args)), **kwargs}
        for name, info in execution_info.items():
            kwargs = {arg: results[arg] for arg in info["arguments"]}
            result = info["func"](**kwargs)
            results[name] = result

        out = tuple(results[target] for target in targets)
        return out

    return concatenated


def _format_list_linewise(list_):
    formatted_list = '",\n    "'.join(list_)
    return textwrap.dedent(
        """
        [
            "{formatted_list}",
        ]
        """
    ).format(formatted_list=formatted_list)
