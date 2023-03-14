import functools
import inspect
import re
import warnings
from typing import Any
from typing import Callable
from typing import Literal
from typing import Optional
from typing import Union

from dags import concatenate_functions
from dags.dag import _create_arguments_of_concatenated_function
from dags.dag import _get_free_arguments
from dags.dag import create_dag
from dags.signature import rename_arguments
from flatten_dict import flatten
from flatten_dict import unflatten
from flatten_dict.reducers import make_reducer
from flatten_dict.splitters import make_splitter

# Type aliases
NestedFunctionDict = dict[str, Union[Callable, "NestedFunctionDict"]]
FlatFunctionDict = dict[str, Callable]

NestedTargetDict = dict[str, Union[None, "NestedTargetDict"]]
FlatTargetList = list[str]

NestedInputStructureDict = dict[str, Union[None, "NestedInputStructureDict"]]
FlatInputStructureDict = dict[str, None]

NestedInputDict = dict[str, Union[Any, "NestedInputDict"]]
NestedOutputDict = dict[str, Union[Any, "NestedOutputDict"]]

NestedStrDict = dict[str, Union[Any, "NestedStrDict"]]
FlatStrDict = dict[str, Any]

GlobalOrLocal = Literal["global", "local"]

# Constants
_python_identifier = r"[a-zA-Z_][a-zA-Z0-9_]*"
_qualified_name_delimiter = "__"
_qualified_name = (
    f"{_python_identifier}(?:{_qualified_name_delimiter}{_python_identifier})+"
)

# Reducers and splitters to flatten/unflatten dicts with qualified names as keys
_qualified_name_reducer = make_reducer(delimiter=_qualified_name_delimiter)
_qualified_name_splitter = make_splitter(delimiter=_qualified_name_delimiter)


# Functions
def concatenate_functions_tree(
    functions: NestedFunctionDict,
    targets: Optional[NestedTargetDict],
    input_structure: NestedInputStructureDict,
    name_clashes: Literal["raise", "warn", "ignore"] = "raise",
    enforce_signature: bool = True,
) -> Callable:
    """
    Combine functions to one function that generates targets.

    Functions can depend on the output of other functions as inputs, as long as the
    dependencies can be described by a directed acyclic graph (DAG). Functions that are
    not required to produce the `targets` will simply be ignored.

    The combined function must be called with a dictionary that matches the given
    `input_structure`.

    Args:
        functions:
            The nested dictionary of functions that will be concatenated.
            **Example:** `{ "f1": f1, "nested": {"f2": f2, "f3": f3 } }`
        targets:
            The nested dictionary of targets that will later be computed. If `None`,
            all variables are returned.
            **Example:** `{ "f1": None, "namespace": {"f2": None } }`
        input_structure:
            A nested dictionary that describes the structure of the inputs.
            **Example:** `{ "a": None, "b": None, "nested": {"c": None } }`
        name_clashes:
            How to handle name clashes between functions and input_structure. If
            `"raise"`, a ValueError is raised. If `"warn"`, a warning is raised. If
            `"ignore"`, the issue is ignored.
        enforce_signature:
            If `True`, the signature of the concatenated function is enforced.
            Otherwise, it is only provided for introspection purposes. Enforcing the
            signature has a small runtime overhead.

    Returns:
        A function that produces `targets` when called with suitable arguments.
    """

    flat_functions = _flatten_functions_and_rename_parameters(
        functions, input_structure, name_clashes
    )
    flat_targets = _flatten_targets(targets)

    concatenated_function = concatenate_functions(
        flat_functions,
        flat_targets,
        return_type="dict",
        enforce_signature=enforce_signature,
    )

    @functools.wraps(concatenated_function)
    def wrapper(inputs: NestedInputDict) -> NestedOutputDict:
        flat_inputs = _flatten_str_dict(inputs)
        flat_outputs = concatenated_function(**flat_inputs)
        return _unflatten_str_dict(flat_outputs)

    return wrapper


def _flatten_functions_and_rename_parameters(
    functions: NestedFunctionDict,
    input_structure: NestedInputStructureDict,
    name_clashes: Literal["raise", "warn", "ignore"] = "raise",
) -> FlatFunctionDict:
    flat_functions = _flatten_str_dict(functions)
    flat_input_structure = _flatten_str_dict(input_structure)

    _check_functions_and_input_overlap(
        flat_functions, flat_input_structure, name_clashes
    )

    for name, function in flat_functions.items():
        namespace = _qualified_name_delimiter.join(
            name.split(_qualified_name_delimiter)[:-1]
        )

        renamed = rename_arguments(
            function,
            mapper=_create_parameter_name_mapper(
                flat_functions,
                flat_input_structure,
                namespace,
                function,
            ),
        )

        flat_functions[name] = renamed

    return flat_functions


def _check_functions_and_input_overlap(
    flat_functions: FlatFunctionDict,
    input_structure: FlatInputStructureDict,
    name_clashes: Literal["raise", "warn", "ignore"],
) -> None:
    overlap = set(flat_functions.keys()) & set(input_structure.keys())

    if overlap:
        message = f"These names are both functions and inputs: {', '.join(overlap)}."

        if name_clashes == "raise":
            raise ValueError(message)
        elif name_clashes == "warn":
            warnings.warn(message, stacklevel=2)
        elif name_clashes == "ignore":
            pass


def _create_parameter_name_mapper(
    flat_functions: FlatFunctionDict,
    flat_input_structure: FlatInputStructureDict,
    namespace: str,
    function: Callable,
) -> dict[str, str]:
    return {
        old_name: _map_parameter(
            flat_functions, flat_input_structure, namespace, old_name
        )
        for old_name in _get_free_arguments(function)
    }


def _map_parameter(
    flat_functions: FlatFunctionDict,
    flat_input_structure: FlatInputStructureDict,
    namespace: str,
    parameter_name: str,
) -> str:
    """
    Maps a parameter name to a qualified name that uniquely identifies the requested
    function or input.

    If the parameter is already a qualified name, it is returned as is. Otherwise,
    we look for a function or input with the same name in the current namespace. If
    it is not found, we look for a function or input with the same name in the top
    level. If it is still not found, we raise an error.

    Args:
        flat_functions:
            The flattened functions.
        flat_input_structure:
            The flattened input structure.
        namespace:
            The current namespace.
        parameter_name:
            The name of the parameter to map.

    Returns:
        The qualified name of the requested function or input.

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
    raise ValueError(f"Cannot resolve parameter {parameter_name}")


def create_input_structure_tree(
    functions: NestedFunctionDict,
    targets: Optional[NestedTargetDict] = None,
    namespace_of_inputs: GlobalOrLocal = "local",
) -> NestedInputStructureDict:
    """
    Creates a template that represents the structure of the input dictionary that will
    be passed to the function created by `concatenate_functions_tree`.

    Args:
        functions:
            The nested dictionary of functions that will be concatenated.
            **Example:** `{ "f1": f1, "namespace": {"f2": f2, "f3": f3 } }`
        targets:
            The nested dictionary of targets that will later be computed.
            **Example:** `{ "f1": None, "namespace": {"f2": None } }`
        namespace_of_inputs:
            Controls where the inputs are added to the template, if the parameter name
            does not uniquely identify its location. If "local", the inputs are added
            to the current namespace. If "global", the inputs are added to the top
            level.
    Returns:
        A template that represents the structure of the input dictionary.
    """

    flat_functions = _flatten_str_dict(functions)
    flat_input_structure: FlatInputStructureDict = {}

    for path, func in flat_functions.items():
        namespace = _qualified_name_delimiter.join(
            path.split(_qualified_name_delimiter)[:-1]
        )
        parameter_names = dict(inspect.signature(func).parameters).keys()

        for parameter_name in parameter_names:
            parameter_path = _link_parameter_to_function_or_input(
                flat_functions, namespace, parameter_name, namespace_of_inputs
            )

            if parameter_path not in flat_functions:
                flat_input_structure[parameter_path] = None

    nested_input_structure = _unflatten_str_dict(flat_input_structure)

    # If no targets are specified, all inputs are needed
    if targets is None:
        return nested_input_structure

    # Compute transitive hull of inputs needed for given targets
    flat_renamed_functions = _flatten_functions_and_rename_parameters(
        functions, nested_input_structure
    )
    flat_targets = _flatten_targets(targets)
    dag = create_dag(flat_renamed_functions, flat_targets)
    parameters = _create_arguments_of_concatenated_function(flat_renamed_functions, dag)

    return _unflatten_str_dict({parameter: None for parameter in parameters})


def _flatten_str_dict(str_dict: NestedStrDict) -> FlatStrDict:
    return flatten(str_dict, reducer=_qualified_name_reducer)


def _unflatten_str_dict(str_dict: FlatStrDict) -> NestedStrDict:
    return unflatten(str_dict, splitter=_qualified_name_splitter)


def _flatten_targets(targets: Optional[NestedTargetDict]) -> Optional[FlatTargetList]:
    if targets is None:
        return None

    return list(_flatten_str_dict(targets).keys())


def _link_parameter_to_function_or_input(
    flat_functions: FlatFunctionDict,
    namespace: str,
    parameter_name: str,
    namespace_of_inputs: GlobalOrLocal = "local",
) -> str:
    """
    Returns the path to the function/input that the parameter points to.

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
    Returns:
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
    else:
        return namespaced_parameter


def _is_python_identifier(s: str) -> bool:
    return bool(re.fullmatch(_python_identifier, s))


def _is_qualified_name(s: str) -> bool:
    return bool(re.fullmatch(_qualified_name, s))
