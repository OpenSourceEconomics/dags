import re
from typing import Literal, Optional, Callable, Union

NestedFunctionDict = dict[str, Union[Callable, "NestedFunctionDict"]]
NestedTargetDict = dict[str, Union[str, list[str], "NestedTargetDict"]]
NestedInputStructureDict = dict[str, Union[None, "NestedInputStructureDict"]]


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
        targets: Optional[NestedTargetDict],
        level_of_inputs: Literal["bottom", "top"] = "bottom",
) -> NestedInputStructureDict:
    """

    Args:
        functions:
        targets:
        level_of_inputs:

    Returns:

    """

    pass


_python_identifier = r"[a-zA-Z_][a-zA-Z0-9_]*"
_qualified_name = f"{_python_identifier}(?:__{_python_identifier})+"


def _is_python_identifier(s: str) -> bool:
    return bool(re.fullmatch(_python_identifier, s))


def _is_qualified_name(s: str) -> bool:
    return bool(re.fullmatch(_qualified_name, s))
