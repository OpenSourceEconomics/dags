import inspect
import re
from typing import Callable
from typing import Literal
from typing import Optional
from typing import Union

from flatten_dict import flatten
from flatten_dict import unflatten

# Type aliases

NestedFunctionDict = dict[str, Union[Callable, "NestedFunctionDict"]]
FlatFunctionDict = dict[tuple[str], Callable]
NestedTargetDict = dict[str, Union[str, list[str], "NestedTargetDict"]]
FlatTargetDict = dict[tuple[str], Union[str, list[str]]]
NestedInputStructureDict = dict[str, Union[None, "NestedInputStructureDict"]]
FlatInputStructureDict = dict[tuple[str], None]
TopOrNamespace = Literal["top", "namespace"]

# Constants

_python_identifier = r"[a-zA-Z_][a-zA-Z0-9_]*"
_qualified_name_delimiter = "__"
_qualified_name = (
    f"{_python_identifier}(?:{_qualified_name_delimiter}{_python_identifier})+"
)


# Functions


def concatenate_functions_tree(
    functions: NestedFunctionDict,
    targets: Optional[NestedTargetDict],
    input_structure: NestedInputStructureDict,
    name_clashes: Literal["raise", "warn", "ignore"] = "raise",
    enforce_signature: bool = True,
) -> Callable:
    """

    Args:
        functions:
        targets:
        input_structure:
        name_clashes:
        enforce_signature:

    Returns:

    """

    pass


def create_input_structure_tree(
    functions: NestedFunctionDict,
    level_of_inputs: TopOrNamespace = "namespace",
) -> NestedInputStructureDict:
    """
    Creates a template that represents the structure of the input dictionary that will be
    passed to the function created by `concatenate_functions_tree`.

    Args:
        functions:
            The nested dictionary of functions that will be concatenated.
        level_of_inputs:
            Controls where the inputs are added to the template, if the parameter name
            does not uniquely identify its location. If "namespace", the inputs are added
            to the current namespace. If "top", the inputs are added to the top level.
    Returns:
        A template that represents the structure of the input dictionary.
    """

    flat_functions = _flatten_functions(functions)
    flat_input_structure: FlatInputStructureDict = {}

    for path, func in flat_functions.items():
        namespace_path = path[:-1]
        parameter_names = dict(inspect.signature(func).parameters).keys()

        for parameter_name in parameter_names:
            parameter_path = _compute_path_for_parameter(
                flat_functions, namespace_path, parameter_name, level_of_inputs
            )

            if parameter_path not in flat_functions:
                flat_input_structure[parameter_path] = None

    return unflatten(flat_input_structure, splitter="tuple")


def _flatten_functions(functions: NestedFunctionDict) -> FlatFunctionDict:
    return flatten(functions, reducer="tuple")


def _compute_path_for_parameter(
    flat_functions: FlatFunctionDict,
    namespace_path: tuple[str],
    parameter_name: str,
    level_of_inputs: TopOrNamespace = "namespace",
) -> tuple[str]:
    """
    Returns the path to the function/input that the parameter points to.

    If the parameter name has double underscores (e.g. "namespace1__f"), we know it
    represents a qualified name and the path simply consists of the segments of the
    qualified name (e.g. "namespace1, "f").

    Otherwise, we cannot be sure whether the parameter points to a function/input of the
    current namespace or a function/input of the top level. In this case, we
        (1) look for a function with that name in the current namespace,
        (2) look for a function with that name in the top level, and
        (3) assume the parameter points to an input.
    In the third case, `level_of_inputs` determines whether the parameter points to an
    input of the current namespace ("namespace") or an input of the top level ("top").

    Args:
        flat_functions:
            The flat dictionary of functions.
        namespace_path:
            The path to the namespace that contains the function that contains the
            parameter.
        parameter_name:
            The name of the parameter.
        level_of_inputs:
            The level of inputs to assume if the parameter name does not represent a
            function.
    Returns:
        The path to the function/input that the parameter points to.
    """

    parameter_path = tuple(parameter_name.split(_qualified_name_delimiter))

    # Parameter name is definitely a qualified name
    if _is_qualified_name(parameter_name):
        return parameter_path

    # (1) Look for function in the current namespace
    namespace_parameter_path = namespace_path + parameter_path
    if namespace_parameter_path in flat_functions:
        return namespace_parameter_path

    # (2) Look for function in the top level
    if parameter_path in flat_functions:
        return parameter_path

    # (3) Assume parameter points to an unknown input
    if level_of_inputs == "top":
        return parameter_path
    else:
        return namespace_parameter_path


def _is_python_identifier(s: str) -> bool:
    return bool(re.fullmatch(_python_identifier, s))


def _is_qualified_name(s: str) -> bool:
    return bool(re.fullmatch(_qualified_name, s))
