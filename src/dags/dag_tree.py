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


def _is_qualified_name(name: str) -> bool:
    pass
