from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dags.typing import GenericCallable

import functools
import inspect


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


def get_annotations(
    func: GenericCallable,
    eval_str: bool = False,
    default: str = "unknown_type",
) -> dict[str, str]:
    """Thin wrapper around inspect.get_annotations to also handle partialled funcs.

    Args:
        func: The function to get annotations from.
        eval_str: If True, the string type annotations are evaluated.
        default: The default value to use if an annotation is missing.

    Returns
    -------
        A dictionary with the argument names as keys and the type annotations as values.
        The type annotations are strings.

    Raises
    ------
        NonStringAnnotationError: If the type annotations are not strings.

    """
    if isinstance(func, functools.partial):
        annotations = inspect.get_annotations(func.func, eval_str=eval_str)
    else:
        annotations = inspect.get_annotations(func, eval_str=eval_str)
    free_arguments = get_free_arguments(func)
    return {arg: annotations.get(arg, default) for arg in ["return", *free_arguments]}
