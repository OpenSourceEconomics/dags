from __future__ import annotations

import functools
import inspect
import textwrap
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

import networkx as nx

from dags.annotations import (
    get_annotations,
    get_free_arguments,
    verify_annotations_are_strings,
)
from dags.exceptions import (
    AnnotationMismatchError,
    CyclicDependencyError,
    DagsError,
    MissingFunctionsError,
)
from dags.output import aggregated_output, dict_output, list_output, single_output
from dags.signature import with_signature

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from dags.typing import T


class DagsWarning(UserWarning):
    """Base class for all warnings in the dags library."""


@dataclass(frozen=True)
class FunctionExecutionInfo:
    """Information about a function that is needed to execute it.

    Attributes
    ----------
        name: The name of the function.
        func: The function to execute.
        verify_annotations: If True, we verify that the annotations are strings.

    Properties
    ----------
        annotations: The annotations of the function. For standard functions this
            coincides with the __annotations__ attribute of the function. For partialled
            functions, this is a dictionary with the names of the free arguments as keys
            and their expected types as values, as well as the return type of the
            function stored under the key "return". Type annotations must be strings,
            else a NonStringAnnotationError is raised.
        arguments: The names of the arguments of the function.
        argument_annotations: The argument annotations of the function.
        return_annotation: The return annotation of the function.

    Raises
    ------
        NonStringAnnotationError: If `verify_annotations` is `True` and the type
            annotations are not strings.

    """

    name: str
    func: Callable[..., Any]
    verify_annotations: bool = False

    def __post_init__(self) -> None:
        """Verify that the annotations are strings."""
        if self.verify_annotations:
            verify_annotations_are_strings(self.annotations, self.name)

    @functools.cached_property
    def annotations(self) -> dict[str, str]:
        """The annotations of the function."""
        return get_annotations(self.func)

    @property
    def arguments(self) -> list[str]:
        """The names of the arguments of the function."""
        return list(set(self.annotations) - {"return"})

    @property
    def argument_annotations(self) -> dict[str, str]:
        """The argument annotations of the function."""
        return {arg: self.annotations[arg] for arg in self.arguments}

    @property
    def return_annotation(self) -> str:
        """The return annotation of the function."""
        return self.annotations["return"]


def concatenate_functions(
    functions: dict[str, Callable[..., Any]] | list[Callable[..., Any]],
    targets: str | list[str] | None = None,
    *,
    dag: nx.DiGraph[str] | None = None,
    return_type: Literal["tuple", "list", "dict"] = "tuple",
    aggregator: Callable[[T, T], T] | None = None,
    enforce_signature: bool = True,
    set_annotations: bool = False,
    lexsort_key: Callable[[str], Any] | None = None,
) -> Callable[..., Any]:
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
        dag (networkx.DiGraph or None): A DAG of functions. If None, a new DAG is
            created from the functions and targets.
        return_type (str): One of "tuple", "list", "dict". This is ignored if the
            targets are a single string or if an aggregator is provided.
        aggregator (callable or None): Binary reduction function that is used to
            aggregate the targets into a single target.
        enforce_signature (bool): If True, the signature of the concatenated function
            is enforced. Otherwise it is only provided for introspection purposes.
            Enforcing the signature has a small runtime overhead.
        set_annotations (bool): If True, sets the annotations of the concatenated
            function based on those of the functions used to generate the targets. The
            return annotation of the concatenated function reflects the requested return
            type and number of targets (e.g., for two targets returned as a list, the
            return annotation is a list of their respective type hints). Note that this
            is not a valid type annotation and should not be used for type checking. All
            annotations must be strings; otherwise, a NonStringAnnotationError is
            raised. To ensure string annotations, enclose them in quotes or use "from
            __future__ import annotations" at the top of your file. An
            AnnotationMismatchError is raised if annotations differ between functions.
        lexsort_key (callable or None): A function that takes a string and returns a
            value that can be used to sort the nodes. This is used to sort the nodes
            in the topological sort. If None, the nodes are sorted alphabetically.

    Returns
    -------
        function: A function that produces targets when called with suitable arguments.

    Raises
    ------
        - NonStringAnnotationError: If `set_annotations` is `True` and the type
            annotations are not strings.

        - AnnotationMismatchError: If `set_annotations` is `True` and there are
            incompatible annotations in the DAG's components.

    """
    if set_annotations and not isinstance(targets, str) and aggregator is not None:
        warnings.warn(
            "Cannot infer return annotation when using an aggregator on multiple "
            "targets.",
            DagsWarning,
            stacklevel=2,
        )

    if dag is None:
        # Create the DAG.
        dag = create_dag(functions=functions, targets=targets)

    # Build combined function.
    return _create_combined_function_from_dag(
        dag=dag,
        functions=functions,
        targets=targets,
        return_type=return_type,
        aggregator=aggregator,
        enforce_signature=enforce_signature,
        set_annotations=set_annotations,
        lexsort_key=lexsort_key,
    )


def create_dag(
    functions: dict[str, Callable[..., Any]] | list[Callable[..., Any]],
    targets: str | list[str] | None,
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
    _functions, _targets = harmonize_and_check_functions_and_targets(
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
    functions: dict[str, Callable[..., Any]] | list[Callable[..., Any]],
    targets: str | list[str] | None,
    return_type: Literal["tuple", "list", "dict"] = "tuple",
    aggregator: Callable[[T, T], T] | None = None,
    enforce_signature: bool = True,
    set_annotations: bool = False,
    lexsort_key: Callable[[str], Any] | None = None,
) -> Callable[..., Any]:
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
        set_annotations (bool): If True, sets the annotations of the concatenated
            function based on those of the functions used to generate the targets. The
            return annotation of the concatenated function reflects the requested return
            type and number of targets (e.g., for two targets returned as a list, the
            return annotation is a list of their respective type hints). Note that this
            is not a valid type annotation and should not be used for type checking. All
            annotations must be strings; otherwise, a NonStringAnnotationError is
            raised. To ensure string annotations, enclose them in quotes or use "from
            __future__ import annotations" at the top of your file. An
            AnnotationMismatchError is raised if annotations differ between functions.
        lexsort_key (callable or None): A function that takes a string and returns a
            value that can be used to sort the nodes. This is used to sort the nodes
            in the topological sort. If None, the nodes are sorted alphabetically.

    Returns
    -------
        function: A function that produces targets when called with suitable arguments.

    Raises
    ------
        - NonStringAnnotationError: If `set_annotations` is `True` and the type
            annotations are not strings.

        - AnnotationMismatchError: If `set_annotations` is `True` and there are
            incompatible annotations in the DAG's components.

    """
    # Harmonize and check arguments.
    _functions, _targets = harmonize_and_check_functions_and_targets(
        functions,
        targets,
    )

    _arglist = create_arguments_of_concatenated_function(_functions, dag)
    _exec_info = create_execution_info(
        _functions, dag, verify_annotations=set_annotations, lexsort_key=lexsort_key
    )

    # Create the concatenated function that returns all requested targets as a tuple.
    # If set_annotations is True, the return annotation is a tuple of strings,
    # corresponding to the return types of the targets.
    _concatenated = _create_concatenated_function(
        _exec_info,
        _arglist,
        _targets,
        enforce_signature,
        set_annotations,
    )

    # Update the actual return type, as well as the return annotation of the
    # concatenated function.
    out: Callable[..., Any]
    if isinstance(targets, str) or (aggregator is not None and len(_targets) == 1):
        out = single_output(_concatenated, set_annotations=set_annotations)
    elif aggregator is not None:
        out = aggregated_output(_concatenated, aggregator=aggregator)
    elif return_type == "list":
        out = cast(
            "Callable[..., Any]",
            list_output(_concatenated, set_annotations=set_annotations),
        )
    elif return_type == "tuple":
        out = _concatenated
    elif return_type == "dict":
        out = cast(
            "Callable[..., Any]",
            dict_output(_concatenated, keys=_targets, set_annotations=set_annotations),
        )
    else:
        msg = (
            f"Invalid return type {return_type}. Must be 'list', 'tuple', or 'dict'. "
            f"You provided {return_type}."
        )
        raise DagsError(msg)

    return out


def get_ancestors(
    functions: dict[str, Callable[..., Any]] | list[Callable[..., Any]],
    targets: str | list[str] | None,
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
    _functions, _targets = harmonize_and_check_functions_and_targets(
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


def harmonize_and_check_functions_and_targets(
    functions: dict[str, Callable[..., Any]] | list[Callable[..., Any]],
    targets: str | list[str] | None,
) -> tuple[dict[str, Callable[..., Any]], list[str]]:
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
    functions: dict[str, Callable[..., Any]] | list[Callable[..., Any]],
) -> dict[str, Callable[..., Any]]:
    if not isinstance(functions, dict):
        functions_dict = {func.__name__: func for func in functions}
    else:
        functions_dict = functions

    return functions_dict


def _harmonize_targets(
    targets: str | list[str] | None,
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
        raise DagsError(msg)


def _fail_if_functions_are_missing(
    functions: dict[str, Callable[..., Any]],
    targets: list[str],
) -> None:
    targets_not_in_functions = set(targets) - set(functions)
    if targets_not_in_functions:
        formatted = format_list_linewise(list(targets_not_in_functions))
        msg = f"The following targets have no corresponding function:\n{formatted}"
        raise MissingFunctionsError(msg)


def _fail_if_dag_contains_cycle(dag: nx.DiGraph[str]) -> None:
    """Check for cycles in DAG."""
    cycles = list(nx.simple_cycles(dag))

    if len(cycles) > 0:
        formatted = format_list_linewise(cycles)
        msg = f"The DAG contains one or more cycles:\n{formatted}"
        raise CyclicDependencyError(msg)


def _create_complete_dag(
    functions: dict[str, Callable[..., Any]],
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
    functions: dict[str, Callable[..., Any]],
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


def create_execution_info(
    functions: dict[str, Callable[..., Any]],
    dag: nx.DiGraph[str],
    verify_annotations: bool = False,
    lexsort_key: Callable[[str], Any] | None = None,
) -> dict[str, FunctionExecutionInfo]:
    """Create a dictionary with all information needed to execute relevant functions.

    Args:
        functions (dict): Dictionary containing functions to build the DAG.
        dag (networkx.DiGraph): The complete DAG.
        verify_annotations (bool): If True, we verify that the annotations are strings.
        lexsort_key (callable or None): A function that takes a string and returns a
            value that can be used to sort the nodes. This is used to sort the nodes
            in the topological sort. If None, the nodes are sorted alphabetically.

    Returns
    -------
        dict: Dictionary with functions and their arguments for each node in the DAG.
            The functions are already in topological_sort order.

    Raises
    ------
        NonStringAnnotationError: If `verify_annotations` is `True` and the type
            annotations are not strings.

    """
    out = {}
    for node in nx.lexicographical_topological_sort(dag, key=lexsort_key):
        if node in functions:
            out[node] = FunctionExecutionInfo(
                name=node,
                func=functions[node],
                verify_annotations=verify_annotations,
            )
    return out


def _create_concatenated_function(
    execution_info: dict[str, FunctionExecutionInfo],
    arglist: list[str],
    targets: list[str],
    enforce_signature: bool,
    set_annotations: bool,
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
        set_annotations (bool): If True, sets the annotations of the concatenated
            function based on those of the functions used to generate the targets. The
            return annotation of the concatenated function reflects the requested return
            type and number of targets (e.g., for two targets returned as a list, the
            return annotation is a list of their respective type hints). Note that this
            is not a valid type annotation and should not be used for type checking. All
            annotations must be strings; otherwise, a NonStringAnnotationError is
            raised. To ensure string annotations, enclose them in quotes or use "from
            __future__ import annotations" at the top of your file. An
            AnnotationMismatchError is raised if annotations differ between functions.

    Returns
    -------
        The concatenated function

    """
    args: list[str] | dict[str, str]
    return_annotation: type[inspect._empty] | tuple[str, ...]

    if set_annotations:
        args, return_annotation = get_annotations_from_execution_info(
            execution_info,
            arglist=arglist,
            targets=targets,
        )
    else:
        args = arglist
        return_annotation = inspect.Parameter.empty

    @with_signature(
        args=args,
        enforce=enforce_signature,
        return_annotation=return_annotation,
    )
    def concatenated(*args: Any, **kwargs: Any) -> tuple[Any, ...]:
        results = {**dict(zip(arglist, args, strict=False)), **kwargs}
        for name, info in execution_info.items():
            func_kwargs = {arg: results[arg] for arg in info.arguments}
            result = info.func(**func_kwargs)
            results[name] = result

        return tuple(results[target] for target in targets)

    return concatenated


def get_annotations_from_execution_info(
    execution_info: dict[str, FunctionExecutionInfo],
    arglist: list[str],
    targets: list[str],
) -> tuple[dict[str, str], tuple[str, ...]]:
    """Get the (argument and return) annotations of the concatenated function.

    Args:
        execution_info: Dataclass with functions and their arguments for each
            node in the DAG. The functions are already in topological_sort order.
        arglist: The list of arguments of the concatenated function.
        targets: The list of targets of the concatenated function.

    Returns
    -------
        - Dictionary with argument names as keys and their expected types in string
          format as values.
        - The expected type of the return value as a string.

    Raises
    ------
        AnnotationMismatchError: If there are incompatible annotations in the DAG's
            components.

    """
    types: dict[str, str] = {}
    errors: list[str] = []
    for name, info in execution_info.items():
        # We do not need to check whether name is already in types_dict, because the
        # functions in execution_info are topologically sorted, and hence, it is
        # impossible for a function to appear as a dependency of another function
        # before appearing as a function itself.
        types[name] = info.return_annotation

        for arg in set(info.argument_annotations).intersection(types.keys()):
            # Verify that the type information on arg that was retrieved up to this
            # point (earlier_type) is consistent with the type information on arg from
            # the current function info (current_type).
            earlier_type = types[arg]
            current_type = info.argument_annotations[arg]

            # The following condition is a hack to deal with overloaded type
            # annotations. E.g., we may have a function that an int and returns an int,
            # or it takes a float and returns a float. We can achieve that with
            # @overload, but the type hints will be "int | float". If we just checked]
            # for equality, we would get an error if a downstream or upstream function
            # required an int or a float. We will not be able to do much better unless
            # we switch away from string-type annotations or replicate the entire logic
            # of a static type checker, both of which are infeasible at the moment.
            if earlier_type not in current_type and current_type not in earlier_type:
                arg_is_function = arg in execution_info
                if arg_is_function:
                    explanation = f"function {arg} has return type: {earlier_type}."
                else:
                    explanation = (
                        f"type annotation '{arg}: {earlier_type}' is used elsewhere."
                    )

                errors.append(
                    f"function {name} has the argument type annotation '{arg}: "
                    f"{current_type}', but {explanation}"
                )

        types.update(info.argument_annotations)

    if errors:
        raise AnnotationMismatchError(
            "The following type annotations are inconsistent:\n" + "\n".join(errors)
        )

    args_annotations = {arg: types[arg] for arg in arglist}
    return_annotation = tuple(types[target] for target in targets)
    return args_annotations, return_annotation


def format_list_linewise(seq: Sequence[object]) -> str:
    formatted_list = '",\n    "'.join([str(c) for c in seq])
    return textwrap.dedent(
        """
        [
            "{formatted_list}",
        ]
        """,
    ).format(formatted_list=formatted_list)
